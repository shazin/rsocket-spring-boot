package lk.techtalks.rsocket.producer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

@SpringBootApplication
public class ProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);
	}

}

@Controller
class TaxisRSocketController {

	@MessageMapping("taxis")
	public Mono<TaxisResponse> taxis(TaxisRequest taxisRequest) {
		if (!"nano".equalsIgnoreCase(taxisRequest.getType()) && !"mini".equalsIgnoreCase(taxisRequest.getType())) {
			return Mono.error(new IllegalArgumentException("Only Taxi types Nano and Mini supported"));
		}
		return Mono.just(new TaxisResponse(61.39493, 23.30044, "Driver 1", "A "+taxisRequest.getType()+" is on the way to go from "+taxisRequest.getFrom() + " to "+ taxisRequest.getTo() + " @ "+ Instant.now()));
	}

	@MessageMapping("taxis-stream")
	public Flux<TaxisResponse> taxisStream(TaxisRequest taxisRequest) {
		if (!"nano".equalsIgnoreCase(taxisRequest.getType()) && !"mini".equalsIgnoreCase(taxisRequest.getType())) {
			return Flux.error(new IllegalArgumentException("Only Taxi types Nano and Mini supported"));
		}
		return Flux
				.interval(Duration.ofSeconds(1))
				.map(i -> new TaxisResponse(Math.random() * 10 + 1, Math.random() * 20 + 1, "Driver "+ i, "A "+taxisRequest.getType()+" is on the way to go from "+taxisRequest.getFrom() + " to "+ taxisRequest.getTo() + " @ "+ Instant.now()));
	}

	@MessageExceptionHandler
	public Flux<TaxisResponse> error(IllegalArgumentException iae) {
		return Flux.just(new TaxisResponse().withMessage(iae.getMessage()));
	}
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class TaxisRequest {
	private String type;
	private String from;
	private String to;


}

@Data
@NoArgsConstructor
@AllArgsConstructor
class TaxisResponse {

	private Double latitude;
	private Double longitude;
	private String driverName;
	private String message;

	public TaxisResponse withMessage(String msg) {
		this.message = msg;
		return this;
	}

}
