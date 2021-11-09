# Apache pulsar demo

* [Admin rest api](https://pulsar.apache.org/admin-rest-api/?version=2.8.1&apiversion=v2)
* [CLI admin api](https://pulsar.apache.org/docs/en/pulsar-admin)

## Code example

For a simple glimpse of use cases, have a look at `apache-pulsar-demo-consumer/src/test/java/com/nacho/pulsar/demo/consumer/TestCases.java`

## How to use?

There are 2 options:
- Run infra only: `docker-compose -f docker-compose-infra.yml up`
  - In this case you'll need to run the `producer` and `consumer` manually from the ide/maven.
  
- Run infra + 1 producer + 2 consumers: `docker-compose up`

In both cases, you'll get have the following local urls:
* Dashboard: http://localhost:7777/
* Metrics: http://localhost:8080/metrics/
* Grafana: http://localhost:3000/ (admin:admin)
* Prometheus: http://localhost:9090/


## Dealing with lag

In `pulsar`, the **lag** is known as **msgBacklog**. And instead of `reset the offset`, the term is `skip messages`.

You'll need to know the **msgBacklog** of the topic and then **skip** those for a _subscription_.
 
* Get stats: `bin/pulsar-admin topics stats persistent://sample/pulsar/ns1/key_shared-topic`

* Skip: `bin/pulsar-admin topics skip persistent://sample/pulsar/ns1/key_shared-topic  -n 430 -s key_shared-subscription`

### Simulating lag

- Start producing continuously `while true; do echo "Sending..."; curl -XPOST localhost:7070/produce/key-shared; sleep 1; done`
- Stop consumers: Either stop manually from the IDE or do `docker stop apache-pulsar-demo_consumer-1_1 && docker stop apache-pulsar-demo_consumer-2_1`
 
- You'll see lag accumulating in [grafana localBacklog](http://localhost:3000/dashboard/file/namespace.json?refresh=5s&panelId=4&fullscreen&orgId=1&var-cluster=standalone&var-namespace=sample%2Fpulsar%2Fns1&from=now-30m&to=now)
and in [Prometheus](http://localhost:9090/graph?g0.expr=pulsar_msg_backlog%7Btopic%3D%22persistent%3A%2F%2Fsample%2Fpulsar%2Fns1%2Fkey_shared-topic%22%7D&g0.tab=1&g0.stacked=0&g0.show_exemplars=0&g0.range_input=1h).
- To restart the consumers either run it manually from the IDE or do `docker start apache-pulsar-demo_consumer-1_1 && docker start apache-pulsar-demo_consumer-2_1`

## Useful links

Setup:
* https://pulsar.apache.org/docs/en/standalone-docker/
* https://github.com/apache/pulsar/blob/master/docker-compose/standalone-dashboard/docker-compose.yml

Monitoring:
* https://pulsar.apache.org/docs/en/deploy-monitoring/
* https://prometheus.io/docs/prometheus/latest/getting_started/
* https://prometheus.io/docs/prometheus/latest/installation/
* https://github.com/streamnative/apache-pulsar-grafana-dashboard