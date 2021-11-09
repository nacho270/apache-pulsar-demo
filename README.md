# Apache pulsar demo

* Dashboard: http://localhost:7777/
* docker-compose up
* Metrics: http://localhost:8180/metrics/
* Grafana: http://localhost:3000/ (admin:admin)
* Prometheus: http://localhost:9090/


Lag:
    `bin/pulsar-admin topics stats persistent://sample/pulsar/ns1/key_shared-topic`
    Property: "msgBacklog"

Reset offset:
    `bin/pulsar-admin topics skip persistent://sample/pulsar/ns1/key_shared-topic  -n 430 -s key_shared-subscription`