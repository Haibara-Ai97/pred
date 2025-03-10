import json
import logging
import signal
import traceback
from asyncio import Queue
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from multiprocessing import Manager
from functools import partial
from urllib import request

import numpy as np
import pika
from datetime import datetime
from typing import Dict, Any, List

import preprocess.preprocessing as prepare
import predict.predicting as predict
import assess.assessing as assess
from assess.model.sigma import Sigma3Exception
from predict.model.arima import ARIMAException
from modelselector.selector import ModelSelector

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ModelService:
    def __init__(self, host: str = 'localhost', port: int = 5672,
                 username: str = 'guest', password: str = 'guest',
                 virtual_host: str = '/'):
        """初始化模型服务"""
        self.connection_params = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=pika.PlainCredentials(username, password),
            heartbeat=60,  # 心跳超时时间
            blocked_connection_timeout=30
        )
        self.connection = None
        self.channel = None
        self.should_stop = False
        self.queues = {
            'model_requests': 'model_requests',
            'model_responses': 'model_responses',
            'calculate_requests': 'calculate_requests',
            'calculate_responses': 'calculate_responses',
            'predict_requests': 'predict_requests',
            'predict_responses': 'predict_responses',
            'total_requests': 'total_requests',
            'total_responses': 'total_responses',
            'update_requests': 'update_requests',
            'update_responses': 'update_responses',
        }

    def connect(self) -> None:
        """建立RabbitMQ连接"""
        try:
            self.connection = pika.BlockingConnection(self.connection_params)
            self.channel = self.connection.channel()

            # 声明所有队列
            for queue in self.queues.values():
                self.channel.queue_declare(queue=queue, durable=True)

            # 设置预取数量为1，确保公平分发
            self.channel.basic_qos(prefetch_count=1)

            logger.info("Successfully connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            raise

    def handle_model_request(self, ch, method, properties, body: bytes) -> None:
        """处理接收到的请求"""
        try:
            request = json.loads(body.decode())
            logger.info(f"Received request for point: {request['point_name']}")

            # 解析请求参数
            point_name = request['point_name']
            pre_pro_model = request['pre_pro_model']
            predict_model = request['predict_model']
            assess_model = request['assess_model']

            # 初始化模型组件
            preprocess = prepare.Preprocess(point_name)
            prediction = predict.Predict(point_name)
            evaluation = assess.Assess(point_name)

            # 设置模型选择
            models = [pre_pro_model, predict_model, assess_model]
            preprocess.SetModelSelections(models)
            prediction.SetModelSelections(models)
            evaluation.SetModelSelections(models)

            # 获取模型参数
            response = {
                'pred_in': prediction.InputLength(),
                'pred_out': prediction.OutputLength(),
                'baseline': evaluation.BaselineLength(),
                'error': None
            }

            logger.info(f"Successfully processed request for point {point_name}")

        except Exception as e:
            logger.error(f"Error processing request: {str(e)}")
            response = {
                'error': str(e)
            }

        # 发送响应
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=properties.reply_to,
                properties=pika.BasicProperties(
                    correlation_id=properties.correlation_id,
                    delivery_mode=2  # 消息持久化
                ),
                body=json.dumps(response)
            )
        except Exception as e:
            logger.error(f"Failed to send response: {str(e)}")

        # 确认消息已处理
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def handle_model_update(self, ch, method, properties, body: bytes) -> None:
        """处理数据相关请求"""
        request_data = json.loads(body.decode())

        try:
            request = UpdateRequest(request_data)
            logger.info(f"Received data request: {request.request_id}")

            modelselector = ModelSelector(request.point_name)

            try:
                models = modelselector.SelectModelBasedOnOriginData(request.point_type)

                response = {
                    'request_id': request.request_id,
                    'prepro_model': models[0],
                    'predict_model': models[1],
                    'assess_model': models[2],
                    'error': None
                }

            except Exception as e:
                logger.error(f"Error processing ModelUpdate request: {str(e)}")
                response = {
                    'request_id': request_data.get('request_id', ''),
                    'prepro_model': [],
                    'predict_model': [],
                    'assess_model': [],
                    'error': str(e)
                }
                logger.info(f"Preparing to send error response")
            try:
                # 声明固定的响应队列
                ch.queue_declare(queue='update_responses', durable=True)

                # 转换响应为 JSON 并记录
                response_body = json.dumps(response)
                logger.info(f"Sending response to update_responses queue")

                # 发送响应到固定队列
                ch.basic_publish(
                    exchange='',
                    routing_key='update_responses',  # 使用固定的响应队列
                    properties=pika.BasicProperties(
                        correlation_id=request_data.get('request_id', ''),  # 使用请求ID作为correlation_id
                        delivery_mode=2,  # 消息持久化
                        content_type='application/json'
                    ),
                    body=response_body
                )
                logger.info(f"Response published to queue: update_responses")

            except Exception as pub_error:
                logger.error(f"Failed to publish response: {str(pub_error)}", exc_info=True)
                # 即使发送失败也要确认原始消息
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

                # 确认原始消息
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Original message acknowledged")
        except Exception as e:
            logger.error(f"Fatal error in handle_update_request: {str(e)}", exc_info=True)
            # 如果出现致命错误，尝试拒绝消息而不是确认
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                logger.info("Message nacked and requeued due to error")
            except Exception as nack_error:
                logger.error(f"Failed to nack message: {str(nack_error)}", exc_info=True)

    def handle_calculate_request(self, ch, method, properties, body: bytes) -> None:
        """处理数据相关请求"""
        request_data = json.loads(body.decode())
        isModelChange = 0

        try:
            request = CalculateRequest(request_data)
            logger.info(f"Received data request: {request.request_id}")

            preprocess = prepare.Preprocess(request.point_name)
            prediction = predict.Predict(request.point_name)
            evaluation = assess.Assess(request.point_name)

            models = [request.prepro_model, request.predict_model, request.assess_model]
            preprocess.SetModelSelections(models)
            prediction.SetModelSelections(models)
            evaluation.SetModelSelections(models)

            try:
                try:
                    data_origin, data_prepare, data_integrity = gross_calculate(preprocess, request.x, request.y, request.x1,
                                                                               request.y1, request.x_new, request.point_name)
                except Sigma3Exception as e:
                    models = ['ND,ESA,LSR,ML30,NF', models[1], models[2]]
                    isModelChange = 1
                    raise e
                # print(data_origin, data_prepare, data_integrity)

                abnorm_points = []
                for k, v in request.data_origin.items():
                    if len(data_origin) > 20 or len(data_prepare) > 20 :
                        flag1 = sigmas(v, data_prepare)
                        flag2 = outlins(v, data_prepare)
                        flag3 = False

                        if flag1 and flag2 :
                            flag3, _ = distance(v, data_prepare)

                        if flag1 and flag2 and flag3 :
                            abnorm_points.append(k)

                response = {
                    'request_id': request.request_id,
                    'data_origin': data_origin,
                    'data_prepare': data_prepare,
                    'data_integrity': data_integrity,
                    'abnorm_points': abnorm_points,
                    'prepro_model': models[0],
                    'predict_model': models[1],
                    'assess_model': models[2],
                    'model_change': isModelChange,
                    'error': None
                }

            except Exception as e:
                logger.error(f"Error processing calculation request: {str(e)}")
                response = {
                    'request_id': request_data.get('request_id', ''),
                    'data_origin': [],
                    'data_prepare': [],
                    'data_integrity': 0.0,
                    'abnorm_points': [],
                    'prepro_model': models[0],
                    'predict_model': models[1],
                    'assess_model': models[2],
                    'model_change': isModelChange,
                    'error': str(e)
                }
                logger.info(f"Preparing to send error response")
            try:
                # 声明固定的响应队列
                ch.queue_declare(queue='calculate_responses', durable=True)

                # 转换响应为 JSON 并记录
                response_body = json.dumps(response)
                logger.info(f"Sending response to calculate_responses queue")

                # 发送响应到固定队列
                ch.basic_publish(
                    exchange='',
                    routing_key='calculate_responses',  # 使用固定的响应队列
                    properties=pika.BasicProperties(
                        correlation_id=request_data.get('request_id', ''),  # 使用请求ID作为correlation_id
                        delivery_mode=2,  # 消息持久化
                        content_type='application/json'
                    ),
                    body=response_body
                )
                logger.info(f"Response published to queue: calculate_responses")

            except Exception as pub_error:
                logger.error(f"Failed to publish response: {str(pub_error)}", exc_info=True)
                # 即使发送失败也要确认原始消息
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

                # 确认原始消息
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Original message acknowledged")
        except Exception as e:
            logger.error(f"Fatal error in handle_calculate_request: {str(e)}", exc_info=True)
            # 如果出现致命错误，尝试拒绝消息而不是确认
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                logger.info("Message nacked and requeued due to error")
            except Exception as nack_error:
                logger.error(f"Failed to nack message: {str(nack_error)}", exc_info=True)

    def handle_predict_request(self, ch, method, properties, body: bytes) -> None:
        """处理数据相关请求"""
        request_data = json.loads(body.decode())
        isModelChange = 0

        try:
            request = PredictRequest(request_data)
            logger.info(f"Received data request: {request.request_id}")

            preprocess = prepare.Preprocess(request.point_name)
            prediction = predict.Predict(request.point_name)
            evaluation = assess.Assess(request.point_name)

            models = [request.prepro_model, request.predict_model, request.assess_model]
            preprocess.SetModelSelections(models)
            prediction.SetModelSelections(models)
            evaluation.SetModelSelections(models)

            try:
                try:
                    data_predict = predict_calculate(prediction, request.data_prepare, request.point_name)
                    # print(data_origin, data_prepare, data_integrity)
                except ARIMAException as e:
                    models = [models[0], 'LSR,30,1', models[2]]
                    isModelChange = 1
                    raise e

                try:
                    data_assess = assess_calculate(evaluation, request.data_prepare, data_predict, request.point_name)
                    # print(data_origin, data_prepare, data_integrity)
                except Sigma3Exception as e:
                    models = [models[0], models[1], 'ESA,1.28']
                    raise e

                response = {
                    'request_id': request.request_id,
                    'data_predict': data_predict,
                    'data_assess': data_assess,
                    'prepro_model': models[0],
                    'predict_model': models[1],
                    'assess_model': models[2],
                    'model_change': isModelChange,
                    'error': None
                }

            except Exception as e:
                logger.error(f"Error processing calculation request: {str(e)}")
                response = {
                    'request_id': request_data.get('request_id', ''),
                    'data_predict': [],
                    'data_assess': [],
                    'prepro_model': models[0],
                    'predict_model': models[1],
                    'assess_model': models[2],
                    'model_change': isModelChange,
                    'error': str(e)
                }
                logger.info(f"Preparing to send error response")
            try:
                # 声明固定的响应队列
                ch.queue_declare(queue='predict_responses', durable=True)

                # 转换响应为 JSON 并记录
                response_body = json.dumps(response)
                logger.info(f"Sending response to predict_responses queue")

                # 发送响应到固定队列
                ch.basic_publish(
                    exchange='',
                    routing_key='predict_responses',  # 使用固定的响应队列
                    properties=pika.BasicProperties(
                        correlation_id=request_data.get('request_id', ''),  # 使用请求ID作为correlation_id
                        delivery_mode=2,  # 消息持久化
                        content_type='application/json'
                    ),
                    body=response_body
                )
                logger.info(f"Response published to queue: predict_responses")

            except Exception as pub_error:
                logger.error(f"Failed to publish response: {str(pub_error)}", exc_info=True)
                # 即使发送失败也要确认原始消息
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

                # 确认原始消息
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Original message acknowledged")
        except Exception as e:
            logger.error(f"Fatal error in handle_predict_request: {str(e)}", exc_info=True)
            # 如果出现致命错误，尝试拒绝消息而不是确认
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                logger.info("Message nacked and requeued due to error")
            except Exception as nack_error:
                logger.error(f"Failed to nack message: {str(nack_error)}", exc_info=True)

    def handle_total_request(self, ch, method, properties, body: bytes) -> None:
        """处理数据相关请求"""
        request_data = json.loads(body.decode())
        isModelChange = 0

        try:
            request = CalculateRequest(request_data)
            logger.info(f"Received data request: {request.request_id}")

            preprocess = prepare.Preprocess(request.point_name)
            prediction = predict.Predict(request.point_name)
            evaluation = assess.Assess(request.point_name)

            models = [request.prepro_model, request.predict_model, request.assess_model]
            preprocess.SetModelSelections(models)
            prediction.SetModelSelections(models)
            evaluation.SetModelSelections(models)

            try:
                try:
                    data_origin, data_prepare, data_integrity = gross_calculate(preprocess, request.x, request.y,
                                                                                request.x1,
                                                                                request.y1, request.x_new,
                                                                                request.point_name)
                except Sigma3Exception as e:
                    models = ['ND,ESA,LSR,ML30,NF', models[1], models[2]]
                    isModelChange = 1
                    raise e
                # print(data_origin, data_prepare, data_integrity)

                try:
                    data_predict = predict_calculate(prediction, data_prepare, request.point_name)
                    # print(data_origin, data_prepare, data_integrity)
                except ARIMAException as e:
                    models = [models[0], 'LSR,30,1', models[2]]
                    isModelChange = 1
                    raise e

                try:
                    data_assess = assess_calculate(evaluation, data_prepare, data_predict, request.point_name)
                    # print(data_origin, data_prepare, data_integrity)
                except Sigma3Exception as e:
                    models = [models[0], models[1], 'ESA,1.28']
                    raise e


                abnorm_points = []
                for k, v in request.data_origin.items():
                    if len(data_origin) > 20 or len(data_prepare) > 20 :
                        flag1 = sigmas(v, data_prepare)
                        flag2 = outlins(v, data_prepare)
                        flag3 = False

                        if flag1 and flag2 :
                            flag3, _ = distance(v, data_prepare)

                        if flag1 and flag2 and flag3 :
                            abnorm_points.append(k)

                response = {
                    'request_id': request.request_id,
                    'data_origin': data_origin,
                    'data_prepare': data_prepare,
                    'data_integrity': data_integrity,
                    'abnorm_points': abnorm_points,
                    'data_predict': data_predict,
                    'data_assess': data_assess,
                    'prepro_model': models[0],
                    'predict_model': models[1],
                    'assess_model': models[2],
                    'model_change': isModelChange,
                    'error': None
                }

            except Exception as e:
                logger.error(f"Error processing calculation request: {str(e)}")
                response = {
                    'request_id': request_data.get('request_id', ''),
                    'data_origin': [],
                    'data_prepare': [],
                    'data_integrity': 0.0,
                    'abnorm_points': [],
                    'data_predict': [],
                    'data_assess': [],
                    'prepro_model': models[0],
                    'predict_model': models[1],
                    'assess_model': models[2],
                    'model_change': isModelChange,
                    'error': str(e)
                }
                logger.info(f"Preparing to send error response")
            try:
                # 声明固定的响应队列
                ch.queue_declare(queue='total_responses', durable=True)

                # 转换响应为 JSON 并记录
                response_body = json.dumps(response)
                logger.info(f"Sending response to total_responses queue")

                # 发送响应到固定队列
                ch.basic_publish(
                    exchange='',
                    routing_key='total_responses',  # 使用固定的响应队列
                    properties=pika.BasicProperties(
                        correlation_id=request_data.get('request_id', ''),  # 使用请求ID作为correlation_id
                        delivery_mode=2,  # 消息持久化
                        content_type='application/json'
                    ),
                    body=response_body
                )
                logger.info(f"Response published to queue: total_responses")

            except Exception as pub_error:
                logger.error(f"Failed to publish response: {str(pub_error)}", exc_info=True)
                # 即使发送失败也要确认原始消息
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

                # 确认原始消息
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Original message acknowledged")
        except Exception as e:
            logger.error(f"Fatal error in handle_calculate_request: {str(e)}", exc_info=True)
            # 如果出现致命错误，尝试拒绝消息而不是确认
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                logger.info("Message nacked and requeued due to error")
            except Exception as nack_error:
                logger.error(f"Failed to nack message: {str(nack_error)}", exc_info=True)

    def start(self) -> None:
        """启动服务"""
        try:
            self.connect()

            # 设置消费者回调
            self.channel.basic_consume(
                queue=self.queues['model_requests'],
                on_message_callback=self.handle_model_request
            )
            self.channel.basic_consume(
                queue=self.queues['calculate_requests'],
                on_message_callback=self.handle_calculate_request
            )
            self.channel.basic_consume(
                queue=self.queues['predict_requests'],
                on_message_callback=self.handle_predict_request
            )
            self.channel.basic_consume(
                queue=self.queues['total_requests'],
                on_message_callback=self.handle_total_request
            )
            self.channel.basic_consume(
                queue=self.queues['update_requests'],
                on_message_callback=self.handle_model_update
            )

            logger.info("Started consuming messages. Waiting for requests...")

            # 开始消费消息
            self.channel.start_consuming()

        except KeyboardInterrupt:
            logger.info("Received shutdown signal, stopping service...")
            self.stop()
        except Exception as e:
            logger.error(f"Service error: {str(e)}")
            self.stop()

    def stop(self) -> None:
        """停止服务"""
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            logger.info("Service stopped")
        except Exception as e:
            logger.error(f"Error stopping service: {str(e)}")


class ParallelModelService:
    def __init__(self, host: str = 'localhost', port: int = 5672,
                 username: str = 'guest', password: str = 'guest',
                 virtual_host: str = '/', max_workers: int = None):
        """初始化模型服务"""
        self.connection_params = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=pika.PlainCredentials(username, password),
            heartbeat=60,  # 心跳超时时间
            blocked_connection_timeout=30
        )
        self.connection = None
        self.channel = None
        self.should_stop = False
        self.queues = {
            'model_requests': 'model_requests',
            'model_responses': 'model_responses',
            'calculate_requests': 'calculate_requests',
            'calculate_responses': 'calculate_responses',
            'predict_requests': 'predict_requests',
            'predict_responses': 'predict_responses',
        }

        self.max_worker = max_workers or (multiprocessing.cpu_count() - 1)
        self.process_pool = None

    def connect(self) -> None:
        """建立RabbitMQ连接"""
        try:
            self.connection = pika.BlockingConnection(self.connection_params)
            self.channel = self.connection.channel()

            # 声明所有队列
            for queue in self.queues.values():
                self.channel.queue_declare(queue=queue, durable=True)

            # 设置预取数量为1，确保公平分发
            self.channel.basic_qos(prefetch_count=1)

            logger.info("Successfully connected to RabbitMQ")
            self.process_pool = ProcessPoolExecutor(max_workers=self.max_worker)
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            raise

    def handle_model_request(self, ch, method, properties, body: bytes) -> None:
        """处理接收到的请求"""
        try:
            request = json.loads(body.decode())
            logger.info(f"Received request for point: {request['point_name']}")

            # 解析请求参数
            point_name = request['point_name']
            pre_pro_model = request['pre_pro_model']
            predict_model = request['predict_model']
            assess_model = request['assess_model']

            # 初始化模型组件
            preprocess = prepare.Preprocess(point_name)
            prediction = predict.Predict(point_name)
            evaluation = assess.Assess(point_name)

            # 设置模型选择
            models = [pre_pro_model, predict_model, assess_model]
            preprocess.SetModelSelections(models)
            prediction.SetModelSelections(models)
            evaluation.SetModelSelections(models)

            # 获取模型参数
            response = {
                'pred_in': prediction.InputLength(),
                'pred_out': prediction.OutputLength(),
                'baseline': evaluation.BaselineLength(),
                'error': None
            }

            logger.info(f"Successfully processed request for point {point_name}")

        except Exception as e:
            logger.error(f"Error processing request: {str(e)}")
            response = {
                'error': str(e)
            }

        # 发送响应
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=properties.reply_to,
                properties=pika.BasicProperties(
                    correlation_id=properties.correlation_id,
                    delivery_mode=2  # 消息持久化
                ),
                body=json.dumps(response)
            )
        except Exception as e:
            logger.error(f"Failed to send response: {str(e)}")

        # 确认消息已处理
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def handle_calculate_request(self, ch, method, properties, body: bytes) -> None:
        """处理数据相关请求"""
        request_data = json.loads(body.decode())

        try:
            request = CalculateRequest(request_data)
            logger.info(f"Received data request: {request.request_id}")

            try:
                # future = self.process_pool.submit(
                #     self.parallel_gross_calculate,
                #     preprocess,
                #     request.x,
                #     request.y,
                #     request.x1,
                #     request.y1,
                #     request.x_new,
                #     request.point_name
                # )
                # print(data_origin, data_prepare, data_integrity)
                future = self.process_pool.submit(parallel_gross_calculate, request)

                data_origin, data_prepare, data_integrity = future.result()

                abnorm_points = []
                for k, v in request.data_origin.items():
                    if len(data_origin) > 20 or len(data_prepare) > 20:
                        flag1 = sigmas(v, data_prepare)
                        flag2 = outlins(v, data_prepare)
                        flag3 = False

                        if flag1 and flag2:
                            flag3, _ = distance(v, data_prepare)

                        if flag1 and flag2 and flag3:
                            abnorm_points.append(k)

                response = {
                    'request_id': request.request_id,
                    'data_origin': data_origin,
                    'data_prepare': data_prepare,
                    'data_integrity': data_integrity,
                    'abnorm_points': abnorm_points,
                    'error': None
                }

            except Exception as e:
                logger.error(f"Error processing calculation request: {str(e)}")
                response = {
                    'request_id': request_data.get('request_id', ''),
                    'data_origin': [],
                    'data_prepare': [],
                    'data_integrity': 0.0,
                    'abnorm_points': [],
                    'error': str(e)
                }
                logger.info(f"Preparing to send error response")
            try:
                # 声明固定的响应队列
                ch.queue_declare(queue='calculate_responses', durable=True)

                # 转换响应为 JSON 并记录
                response_body = json.dumps(response)
                logger.info(f"Sending response to calculate_responses queue")

                # 发送响应到固定队列
                ch.basic_publish(
                    exchange='',
                    routing_key='calculate_responses',  # 使用固定的响应队列
                    properties=pika.BasicProperties(
                        correlation_id=request_data.get('request_id', ''),  # 使用请求ID作为correlation_id
                        delivery_mode=2,  # 消息持久化
                        content_type='application/json'
                    ),
                    body=response_body
                )
                logger.info(f"Response published to queue: calculate_responses")

            except Exception as pub_error:
                logger.error(f"Failed to publish response: {str(pub_error)}", exc_info=True)
                # 即使发送失败也要确认原始消息
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

                # 确认原始消息
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Original message acknowledged")
        except Exception as e:
            logger.error(f"Fatal error in handle_calculate_request: {str(e)}", exc_info=True)
            # 如果出现致命错误，尝试拒绝消息而不是确认
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                logger.info("Message nacked and requeued due to error")
            except Exception as nack_error:
                logger.error(f"Failed to nack message: {str(nack_error)}", exc_info=True)

    def handle_predict_request(self, ch, method, properties, body: bytes) -> None:
        """处理数据相关请求"""
        request_data = json.loads(body.decode())

        try:
            request = PredictRequest(request_data)
            logger.info(f"Received data request: {request_data['request_id']}")

            try:

                future = self.process_pool.submit(parallel_predict_calculate, request)
                # print(data_origin, data_prepare, data_integrity)

                data_predict, data_assess = future.result()

                response = {
                    'request_id': request.request_id,
                    'data_predict': data_predict,
                    'data_assess': data_assess,
                    'error': None
                }

            except Exception as e:
                logger.error(f"Error processing calculation request: {str(e)}")
                response = {
                    'request_id': request_data.get('request_id', ''),
                    'data_predict': [],
                    'data_assess': [],
                    'error': str(e)
                }
                logger.info(f"Preparing to send error response")
            try:
                # 声明固定的响应队列
                ch.queue_declare(queue='predict_responses', durable=True)

                # 转换响应为 JSON 并记录
                response_body = json.dumps(response)
                logger.info(f"Sending response to predict_responses queue")

                # 发送响应到固定队列
                ch.basic_publish(
                    exchange='',
                    routing_key='predict_responses',  # 使用固定的响应队列
                    properties=pika.BasicProperties(
                        correlation_id=request_data.get('request_id', ''),  # 使用请求ID作为correlation_id
                        delivery_mode=2,  # 消息持久化
                        content_type='application/json'
                    ),
                    body=response_body
                )
                logger.info(f"Response published to queue: predict_responses")

            except Exception as pub_error:
                logger.error(f"Failed to publish response: {str(pub_error)}", exc_info=True)
                # 即使发送失败也要确认原始消息
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

                # 确认原始消息
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Original message acknowledged")
        except Exception as e:
            logger.error(f"Fatal error in handle_predict_request: {str(e)}", exc_info=True)
            # 如果出现致命错误，尝试拒绝消息而不是确认
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                logger.info("Message nacked and requeued due to error")
            except Exception as nack_error:
                logger.error(f"Failed to nack message: {str(nack_error)}", exc_info=True)

    def start(self) -> None:
        """启动服务"""
        try:
            self.connect()

            # 设置消费者回调
            self.channel.basic_consume(
                queue=self.queues['model_requests'],
                on_message_callback=self.handle_model_request
            )
            self.channel.basic_consume(
                queue=self.queues['calculate_requests'],
                on_message_callback=self.handle_calculate_request
            )
            self.channel.basic_consume(
                queue=self.queues['predict_requests'],
                on_message_callback=self.handle_predict_request
            )

            logger.info("Started consuming messages. Waiting for requests...")

            # 开始消费消息
            self.channel.start_consuming()

        except KeyboardInterrupt:
            logger.info("Received shutdown signal, stopping service...")
            self.stop()
        except Exception as e:
            logger.error(f"Service error: {str(e)}")
            self.stop()

    def stop(self) -> None:
        """停止服务"""
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            logger.info("Service stopped")
        except Exception as e:
            logger.error(f"Error stopping service: {str(e)}")

def parallel_gross_calculate(request):
    """并行处理gross_calculate计算"""
    preprocess = prepare.Preprocess(request.point_name)
    prediction = predict.Predict(request.point_name)
    evaluation = assess.Assess(request.point_name)

    models = [request.prepro_model, request.predict_model, request.assess_model]
    preprocess.SetModelSelections(models)
    prediction.SetModelSelections(models)
    evaluation.SetModelSelections(models)
    return gross_calculate(preprocess,
                           request.x,
                           request.y,
                           request.x1,
                           request.y1,
                           request.x_new,
                           request.point_name)

def parallel_predict_calculate(request):
    """并行处理predict_calculate计算"""
    preprocess = prepare.Preprocess(request.point_name)
    prediction = predict.Predict(request.point_name)
    evaluation = assess.Assess(request.point_name)

    models = [request.prepro_model, request.predict_model, request.assess_model]
    preprocess.SetModelSelections(models)
    prediction.SetModelSelections(models)
    evaluation.SetModelSelections(models)
    return predict_calculate(prediction, evaluation, request.data_prepare, request.point_name)


def gross_calculate(model, x, y, x1, y1, x_new, point_name):
    data_origin = []
    data_prepare = []
    data_integrity = 0.0

    try:
        data_origin, data_integrity = model.ResampleFilteredData(x=x, y=y, x_new=x_new)
    except AssertionError as e:
        print('点位{0}重采样1处理异常{1}'.format(point_name,e))
    try:
        # 从原始数据中根据采样点 x_new 去除噪声和异常并采样
        data_prepare, data_integrity = model.ResampleFilteredData(x=x1, y=y1,
                                                                  x_new=x_new)
    except AssertionError as e:
        print('点位{0}预处理过程异常:{1}'.format(point_name, e))

    return data_origin, data_prepare, data_integrity

def predict_calculate(predict, data_prepare, point_name):
    data_predict = []
    y = [data_prepare[-1]]

    try:
        data_predict = predict.Predict(data_prepare)
    except Exception as e:
        print('点位{0}预测异常{1}'.format(point_name, e))

    return data_predict

def assess_calculate(assess, data_prepare, data_predict, point_name):
    data_assess = []
    y = [data_prepare[-1]]

    try:
        data_assess = assess.AssessValue(data_predict, data_prepare, y)
    except Exception as e:
        print('点位{0}评估异常{1}'.format(point_name, e))

    return data_assess[0]



def worker_process(requset_data):
    try:
        point_name = requset_data['point_name']
        x = requset_data['x']
        y = requset_data['y']
        x1 = requset_data['x1']
        y1 = requset_data['y1']
        x_new = requset_data['x_new']
        prepro_model = requset_data['prepro_model']
        predict_model = requset_data['predict_model']
        assess_model = requset_data['assess_model']

        preprocess = prepare.Preprocess(point_name)
        prediction = predict.Predict(point_name)
        evaluation = assess.Assess(point_name)

        models = [prepro_model, predict_model, assess_model]
        preprocess.SetModelSelections(models)
        prediction.SetModelSelections(models)
        evaluation.SetModelSelections(models)

        data_origin, data_prepare, data_integrity = gross_calculate(
            preprocess, x, y, x1, y1, x_new, point_name
        )

        return {
            'request_id': requset_data['request_id'],
            'data_origin': data_origin,
            'data_prepare': data_prepare,
            'data_integrity': data_integrity,
            'error': None
        }
    except Exception as e:
        logger.error(f"Error processing calculation request: {str(e)}")
        return {
            'request_id': requset_data['request_id'],
            'data_origin': [],
            'data_prepare': [],
            'data_integrity': 0.0,
            'error': str(e)
        }

def sigmas(origin, prepares):
    # 数据维度
    resthread1 = []
    resthread2 = []
    for i in range(len(prepares[0])):
        # 存储当前维度的数据
        tem = []
        for j in range(len(prepares)):
            tem.append(prepares[j][i])
        ymean = np.mean(tem)
        ystd = np.std(tem)
        threshold1 = ymean - 3 * ystd
        threshold2 = ymean + 3 * ystd
        resthread1.append(threshold1)
        resthread2.append(threshold2)
    flag = False
    for i in range(len(origin)):
        if origin[i] < resthread1[i] or origin[i] > resthread2[i]:
            flag = True
            break
    return flag


def distance(origin, prepares):
    # 数据维度
    resmean = []
    for i in range(len(prepares[0])):
        # 存储当前维度的数据
        tem = []
        for j in range(len(prepares)):
            tem.append(prepares[j][i])
        # 求平均值并保留到小数点后四位
        ymean = float(format(np.mean(tem), '.4f'))
        resmean.append(ymean)
    flag = False
    # dis 欧式距离
    dis = np.linalg.norm(np.array(resmean, dtype=float) - np.array(origin, dtype=float))
    # 平均值的模长
    dis2 = np.linalg.norm(np.array(resmean, dtype=float))

    # 原始值远大于平均值
    if dis > dis2:
        if dis2 != 0 and abs(dis / dis2) > 5:
            flag = True
            print('欧式距离是与原本值之比为\'{0}\' '.format(str(dis / dis2)))
    # 原始值远小于平均值，
    else:
        if dis2 != 0 and dis / dis2 > 0.95:
            flag = True
            print('欧式距离是与原本值之比为\'{0}\' '.format(str(dis / dis2)))

    return flag, resmean


def outlins(origin, prepares):
    Lower = []
    Upper = []
    for i in range(len(prepares[0])):
        df = []
        for j in range(len(prepares)):
            df.append(prepares[j][i])
        # 1st quartile (25%)
        q1 = np.percentile(df, 25)
        q3 = np.percentile(df, 75)
        iqr = q3 - q1
        Lower_tail = q1 - 3.0 * iqr
        Upper_tail = q3 + 3.0 * iqr
        Lower.append(Lower_tail)
        Upper.append(Upper_tail)
        # outlier step
    flag = False
    for i in range(len(origin)):
        if (origin[i] < Lower[i] or origin[i] > Upper[i]):
            flag = True
            break
    return flag

def main():
    # 串行计算服务配置
    service_config = {
        'host': 'localhost',
        'port': 5672,
        'username': 'guest',
        'password': 'guest',
        'virtual_host': '/'
    }

    # 创建并启动服务
    service = ModelService(**service_config)
    try:
        service.start()
    except Exception as e:
        logger.error(f"Service failed: {str(e)}")
    finally:
        service.stop()

    # 并行计算服务配置
    # service_config = {
    #     'host': 'localhost',
    #     'port': 5672,
    #     'username': 'guest',
    #     'password': 'guest',
    #     'virtual_host': '/',
    #     'max_workers': 10,
    # }
    # service = ParallelModelService(**service_config)
    #
    # try:
    #     service.start()
    # except Exception as e:
    #     logger.error(f"Error starting service: {str(e)}")
    # finally:
    #     service.stop()


class CalculateRequest:
    def __init__(self, data: Dict[str, Any]):
        self.request_id: str = data['request_id']
        self.x: List[float] = data['x']
        self.y: List[List[float]] = data['y']
        self.x1: List[float] = data['x1']
        self.y1: List[List[float]] = data['y1']
        self.x_new: List[float] = data['xNew']
        self.prepro_model: str = data['prepro_model']
        self.predict_model: str = data['predict_model']
        self.assess_model: str = data['assess_model']
        self.point_name: str = data['point_name']
        self.data_origin: Dict[str, List[float]] = data['data_origin']

    def __getstate__(self):
        # 返回可以被序列化的字典
        return {
            'request_id': self.request_id,
            'x': self.x,
            'y': self.y,
            'x1': self.x1,
            'y1': self.y1,
            'x_new': self.x_new,
            'prepro_model': self.prepro_model,
            'predict_model': self.predict_model,
            'assess_model': self.assess_model,
            'point_name': self.point_name,
            'data_origin': self.data_origin
        }

    def __setstate__(self, state):
        # 从序列化的字典中恢复属性
        self.request_id = state['request_id']
        self.x = state['x']
        self.y = state['y']
        self.x1 = state['x1']
        self.y1 = state['y1']
        self.x_new = state['x_new']
        self.prepro_model = state['prepro_model']
        self.predict_model = state['predict_model']
        self.assess_model = state['assess_model']
        self.point_name = state['point_name']
        self.data_origin = state['data_origin']


class PredictRequest:
    def __init__(self, data: Dict[str, Any]):
        self.request_id: str = data['request_id']
        self.data_prepare: List[List[float]] = data['data_prepare']
        self.prepro_model: str = data['prepro_model']
        self.predict_model: str = data['predict_model']
        self.assess_model: str = data['assess_model']
        self.point_name: str = data['point_name']

    def __getstate__(self):
        # 返回可以被序列化的字典
        return {
            'request_id': self.request_id,
            'prepro_model': self.prepro_model,
            'predict_model': self.predict_model,
            'assess_model': self.assess_model,
            'point_name': self.point_name,
            'data_prepare': self.data_prepare
        }

    def __setstate__(self, state):
        # 从序列化的字典中恢复属性
        self.request_id = state['request_id']
        self.prepro_model = state['prepro_model']
        self.predict_model = state['predict_model']
        self.assess_model = state['assess_model']
        self.point_name = state['point_name']
        self.data_prepare = state['data_prepare']


class UpdateRequest:
    def __init__(self, data: Dict[str, Any]):
        self.request_id: str = data['request_id']
        self.point_name: str = data['point_name']
        self.point_type: str = data['point_type']

    def __getstate__(self):
        return {
            'request_id': self.request_id,
            'point_name': self.point_name,
            'type': self.type
        }

    def __setstate__(self, state):
        self.request_id = state['request_id']
        self.point_name = state['point_name']
        self.type = state['type']


if __name__ == "__main__":
    main()