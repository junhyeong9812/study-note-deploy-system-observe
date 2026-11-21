package xyz.junproject.observe

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class ObserveApplication

fun main(args: Array<String>) {
	runApplication<ObserveApplication>(*args)
}
