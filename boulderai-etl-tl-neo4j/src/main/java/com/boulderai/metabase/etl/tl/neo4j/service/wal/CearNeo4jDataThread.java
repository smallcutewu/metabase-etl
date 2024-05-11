package com.boulderai.metabase.etl.tl.neo4j.service.wal;

public class CearNeo4jDataThread extends Thread{
    public void run()
    {
        Neo4jSyncContext.getInstance().clearNeo4jData();
    }
}
