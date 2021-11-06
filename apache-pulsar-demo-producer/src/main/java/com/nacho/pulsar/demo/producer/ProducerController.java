package com.nacho.pulsar.demo.producer;

import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/produce")
@RequiredArgsConstructor
public class ProducerController {

  private final PulsarProducer pulsarProducer;

  @PostMapping("/shared")
  public ResponseEntity<String> produceShared() throws PulsarClientException {
    pulsarProducer.produceShared();
    return ResponseEntity.ok().build();
  }

  @PostMapping("/key-shared")
  public ResponseEntity<String> produceKeyShared() throws PulsarClientException {
    pulsarProducer.produceKeyShared();
    return ResponseEntity.ok().build();
  }
}
