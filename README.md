# Apache Pulsar Source for Numaflow

This document details the setup of an Apache Pulsar source within a [Numaflow](https://numaflow.numaproj.io/) pipeline. The integration of Apache Pulsar as a source allows for the efficient ingestion of data from Apache Pulsar topics into a Numaflow pipeline for further processing and routing to various sinks like Redis.

## Quick Start
Follow this guide to set up an Apache Pulsar source in your Numaflow pipeline.

### Prerequisites
* Ensure that [Numaflow is installed](https://numaflow.numaproj.io/quick-start/) on your Kubernetes cluster.
* Access to an Apache Pulsar cluster. For more information, refer to [Apache Pulsar documentation](https://pulsar.apache.org/docs/en/standalone/).
* Familiarity with creating topics and subscriptions in Apache Pulsar.

### Step-by-Step Guide

#### 1. Configure Apache Pulsar

Ensure that you have an Apache Pulsar topic and a corresponding subscription ready for use. You can create these using the Pulsar admin CLI or the Pulsar dashboard.

#### 2. Deploy a Numaflow Pipeline with Apache Pulsar Source

Create a Kubernetes manifest (e.g., `pulsar-source-pipeline.yaml`) with the following configuration:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: apache-pulsar-source
spec:
  limits:
    readBatchSize: 10
  vertices:
    - name: in
      source:
        udsource:
          container:
            image: "quay.io/numaio/numaflow-go/apache-pulsar-source-go:latest"
            env:
              - name: PULSAR_TOPIC
                value: "test-topic"
              - name: PULSAR_SUBSCRIPTION_NAME
                value: "test-subscription"
              - name: PULSAR_HOST
                value: "pulsar-broker.numaflow-system.svc.cluster.local:6650"
              - name: PULSAR_ADMIN_ENDPOINT
                value: "http://pulsar-broker.numaflow-system.svc.cluster.local:8080"
    - name: redis-sink
      sink:
        udsink:
          container:
            image: "quay.io/numaio/numaflow-sink/redis-e2e-test-sink:v0.5.0"
  edges:
    - from: in
      to: redis-sink
```

Replace `test-topic`, `test-subscription`, and the Pulsar host details with your specific Apache Pulsar configuration.

Apply the configuration to your cluster:
```bash
kubectl apply -f pulsar-source-pipeline.yaml
```

#### 3. Verify the Pipeline

Check the Numaflow pipeline's logs to ensure that it successfully connects to Apache Pulsar and ingests data from the specified topic.

#### 4. Clean up

To delete the Numaflow pipeline:
```bash
kubectl delete -f pulsar-source-pipeline.yaml
```

## Additional Resources

- For detailed guidance on Numaflow, visit the [Numaflow Documentation](https://numaflow.numaproj.io/).
- To learn more about Apache Pulsar and its configuration, refer to the [Apache Pulsar Documentation](https://pulsar.apache.org/docs/en/).
- For Redis sink configuration, see the [Numaflow Redis Sink Documentation](https://numaflow.numaproj.io/sinks/redis/).

This README provides a guide on setting up an Apache Pulsar source within a Numaflow pipeline, detailing prerequisites, a step-by-step setup guide, and additional resources. Remember to replace placeholders like `test-topic`, `test-subscription`, and Pulsar host details with actual values from your Apache Pulsar setup.
