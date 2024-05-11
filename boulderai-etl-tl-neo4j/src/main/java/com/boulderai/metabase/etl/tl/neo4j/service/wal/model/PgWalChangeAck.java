package com.boulderai.metabase.etl.tl.neo4j.service.wal.model;

import com.rabbitmq.client.Channel;

public class PgWalChangeAck {
    private PgWalChange   change;
    private long deliveryTag;
    private Channel channel;

    public PgWalChangeAck(PgWalChange record, long deliveryTag, Channel channel) {
        this.change = record;
        this.deliveryTag = deliveryTag;
        this.channel = channel;
    }

    public PgWalChangeAck(PgWalChange change) {
        this.change = change;
    }
//    public PgWalChangeAck() {
//    }

    public PgWalChange getChange() {
        return change;
    }

    public void setChange(PgWalChange change) {
        this.change = change;
    }

    public long getDeliveryTag() {
        return deliveryTag;
    }

    public void setDeliveryTag(long deliveryTag) {
        this.deliveryTag = deliveryTag;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public void clear() {
        if (channel != null) {
            change.clear();
        }
        change=null;
        channel=null;
    }
}
