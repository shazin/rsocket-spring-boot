package lk.techtalks.rsocket.consumer;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.reactivestreams.Publisher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class ConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);
	}

	@Bean
	public RSocket rSocket() {
		return RSocketFactory
				.connect()
				.mimeType("message/x.rsocket.routing.v0", MimeTypeUtils.APPLICATION_JSON_VALUE)
				.frameDecoder(PayloadDecoder.ZERO_COPY)
				.transport(TcpClientTransport.create(7000))
				.start()
				.block();
	}

	@Bean
	public RSocketRequester rSocketRequester(RSocketStrategies rSocketStrategies) {
		return RSocketRequester.wrap(this.rSocket(), MimeTypeUtils.APPLICATION_JSON, MimeTypeUtils.parseMimeType("message/x.rsocket.routing.v0"), rSocketStrategies);
	}

}

@RestController
class TaxisRestController {

	private final RSocketRequester rSocketRequester;

	TaxisRestController(RSocketRequester rSocketRequester) {
		this.rSocketRequester = rSocketRequester;
	}

	@GetMapping("/taxis/{type}/{from}/{to}")
	public Publisher<TaxisResponse> taxis(@PathVariable String type, @PathVariable String from, @PathVariable String to) {
		return rSocketRequester
				.route("taxis")
				.data(new TaxisRequest(type, from, to))
				.retrieveMono(TaxisResponse.class);
	}

	@GetMapping(value = "/taxis/sse/{type}/{from}/{to}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Publisher<TaxisResponse> taxisStream(@PathVariable String type, @PathVariable String from, @PathVariable String to) {
		return rSocketRequester
				.route("taxis-stream")
				.data(new TaxisRequest(type, from, to))
				.retrieveFlux(TaxisResponse.class);
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
