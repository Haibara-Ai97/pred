package api

import (
	"github.com/gin-gonic/gin"
	"pred/scheduler"
)

func RegisterRoutes(r *gin.Engine, scheduler *scheduler.Scheduler) {
	// 应急任务
	r.POST("/emergency_task", func(c *gin.Context) {
		scheduler.Emergency()
		c.JSON(200, gin.H{
			"message": "Task triggered successfully",
		})
	})

	// 手动触发模型参数更新
	r.POST("/update_models", func(c *gin.Context) {
		scheduler.Update()
		c.JSON(200, gin.H{
			"message": "Models updated successfully",
		})
	})

	// for test
	r.POST("/do_schedule", func(c *gin.Context) {
		var requestBody struct {
			Task string `json:"task"`
		}
		if err := c.BindJSON(&requestBody); err != nil {
			c.JSON(400, gin.H{
				"error": "Invalid request body",
			})
			return
		}
		scheduler.DoScheduler(requestBody.Task)
		c.JSON(200, gin.H{
			"message": "Scheduler executed successfully",
		})
	})
}
