package main

import (
	"github.com/gin-gonic/gin"
	"pred/api"
	"pred/scheduler"
)

func main() {
	cronScheduler := scheduler.NewScheduler()
	go cronScheduler.Start()

	r := gin.Default()
	api.RegisterRoutes(r, cronScheduler)

	r.Run(":8080")
}
