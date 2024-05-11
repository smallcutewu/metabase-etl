package com.boulderai.metabase.etl.tl.neo4j.zookeeper;

import com.boulderai.metabase.etl.tl.neo4j.config.ZookeeperConfig;
import com.boulderai.metabase.etl.tl.neo4j.util.AppCurrentState;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.SessionConnectionStateErrorPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * @ClassName: MetaLeaderSelector
 * @Description: 应用状态主从选择器
 * @author  df.l
 * @date 2022年09月07日
 * @Copyright boulderaitech.com
 */
@Slf4j
public class MetaLeaderSelector {

    @Setter
    private AppStateListener listener;

    private final ZookeeperConfig zookeeperConfig;
    private CuratorFramework client;
    private LeaderLatch selector;
    private  volatile AppCurrentState  appCurrentState=AppCurrentState.SLAVE;
    private volatile  Boolean  connectedZk =false;

    public MetaLeaderSelector(ZookeeperConfig zookeeperConfig) {
        this.zookeeperConfig = zookeeperConfig;
    }

    public void registListener(AppStateListener listener) {
        this.listener = listener;
    }

    /**
     * 创建zk父级永久节点
     */
    private void createParentNode()
    {
        String[] nodeArr = zookeeperConfig.getMasterNodeName().split("/");
        if(nodeArr.length>1)
        {
            String path="";
            for(int i=0;i<nodeArr.length-1;i++)
            {
                if(StringUtils.isNotBlank(nodeArr[i]))
                {
                    path=path+"/"+nodeArr[i];
                    createMasterParentNode( path,CreateMode.PERSISTENT);
                }

            }
        }
    }


    /**
     * 创建父级节点
     * @param path  几点路径
     * @param createMode  节点方式
     */
    private void createMasterParentNode(String path, CreateMode createMode)  {
        try {
            Stat stat=client.checkExists().forPath(path);
            if(stat == null)
            {
                client.create().withMode(createMode).forPath(path);
            }
        } catch (Exception ex) {
            log.error("App create master parent node error!",ex);
        }
    }


    /**
     * 启动开始侦听
     */
    public void start() {
        client = CuratorFrameworkFactory.builder()
                .connectString(zookeeperConfig.getZookeeperUrl())
                .connectionTimeoutMs(zookeeperConfig.getConnectionTimeout())
                .sessionTimeoutMs(zookeeperConfig.getSessionTimeout())
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .connectionStateErrorPolicy(new SessionConnectionStateErrorPolicy())
                .build();

        client.getConnectionStateListenable().addListener((client, newState) -> {
            if (newState == ConnectionState.CONNECTED||
                    newState == ConnectionState.RECONNECTED) {
                log.info("zookeeper 连接成功！");
                connectedZk =true;
            }
            else    if (newState == ConnectionState.SUSPENDED||
                    newState == ConnectionState.LOST) {
                log.info("zookeeper 连接已经断开！");
                connectedZk =false;
            }

        });

       // 连接zk
        client.start();
        createParentNode();

        //设置选择路径
        selector  = new LeaderLatch(client, zookeeperConfig.getMasterNodeName());

        //设置选取侦听器
        selector.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                log.warn("MetaLeaderSelector  change to master !");
                if(listener!=null)
                {
                    if(appCurrentState.getStatus()!=AppCurrentState.MASTER.getStatus())
                    {
                        listener.onChange(AppCurrentState.MASTER);
                        appCurrentState=AppCurrentState.MASTER;
                    }

                }

            }

            @Override
            public void notLeader() {
                log.warn("MetaLeaderSelector  change to slaver !");
                if(listener!=null)
                {
                    //目前单机版可以先注释掉
                    if(appCurrentState.getStatus()!=AppCurrentState.SLAVE.getStatus()) {
//                        listener.onChange(AppCurrentState.SLAVE);
//                        appCurrentState=AppCurrentState.SLAVE;
                    }
                }

            }
        });

        try {
            selector .start();
        } catch (Exception ex) {
            log.error("App start select leader error!",ex);
        }
    }

    public void stop() {
        CloseableUtils.closeQuietly(selector);
        CloseableUtils.closeQuietly(client);
        appCurrentState=AppCurrentState.SLAVE;
        connectedZk=false;
    }

    public Boolean getConnectedZk() {
        return connectedZk;
    }
}
