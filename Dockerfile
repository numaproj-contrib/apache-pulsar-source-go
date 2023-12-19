####################################################################################################
# base
####################################################################################################
FROM alpine:3.12.3 as base
ARG ARCH=amd64

RUN apk update && apk upgrade && \
    apk add ca-certificates && \
    apk --no-cache add tzdata

COPY dist/gcloud-pubsub-source-${ARCH} /bin/gcloud-pubsub-source
RUN chmod +x /bin/gcloud-pubsub-source

####################################################################################################
# gcloud-pubsub-source
####################################################################################################
FROM scratch as gcloud-pubsub-source
ARG ARCH
COPY --from=base /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base /bin/gcloud-pubsub-source /bin/gcloud-pubsub-source
ENTRYPOINT [ "/bin/gcloud-pubsub-source" ]

####################################################################################################
# testbase
####################################################################################################
FROM alpine:3.17 as testbase
RUN apk update && apk upgrade && \
    apk add ca-certificates && \
    apk --no-cache add tzdata

COPY dist/e2eapi /bin/e2eapi
RUN chmod +x /bin/e2eapi


####################################################################################################
# testapi
####################################################################################################
FROM scratch AS e2eapi
COPY --from=testbase /bin/e2eapi .
ENTRYPOINT ["/e2eapi"]
