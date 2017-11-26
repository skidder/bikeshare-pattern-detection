FROM debian:jessie

RUN apt-get update  
RUN apt-get install -y ca-certificates

COPY gbfs-feed-consumer /
CMD ["/gbfs-feed-consumer"]