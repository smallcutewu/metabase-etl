package com.boulderai.metabase.etl.tl.neo4j.util.stat;


import com.boulderai.metabase.etl.tl.neo4j.service.wal.model.CqlValueContainer;
import com.boulderai.metabase.lang.util.SleepUtil;
import com.boulderai.metabase.etl.tl.neo4j.util.DbOperateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @ClassName: WebLogAspect
 * @Description: log操作日志拦截
 * @author  df.l
 * @date 2023年02月22日
 * @Copyright boulderaitech.com
 */
//@Aspect
@Component
@Slf4j
public class WebLogAspect {

    private LinkedBlockingQueue<CqlStat>  cqlStatQueue=new LinkedBlockingQueue<CqlStat>();
    private ExecutorService  executorService;
    private  static  final  String SQL="insert into system_core.pg_cql_stat (cql,parameters,method_type,use_time,method,args_value )  values( ?,?,?,?,?,?)";

    @PostConstruct
    public void init()
    {
        executorService= Executors.newFixedThreadPool(1);
        executorService.submit(new CqlStatRunnable());
    }


     class CqlStatRunnable  implements Runnable{
        private List<CqlStat>  logList=new ArrayList<CqlStat>();
         List<Object[]>  parameters=new ArrayList< Object[]>(100);

        @Override
        public void run() {
            while (true) {
                if(!cqlStatQueue.isEmpty())
                {
                    logList.clear();
                    cqlStatQueue.drainTo(logList,100);
                    if(!logList.isEmpty())
                    {
                        parameters.clear();
                        for (CqlStat cqlStat : logList) {
                            Object[] rowParams={  cqlStat.getCql(),cqlStat.getParameters(),cqlStat.getMethodType(),cqlStat.getUseTime(),cqlStat.getMethod(),cqlStat.getArgsValue()      };
                            parameters.add(rowParams);
                        }
                        DbOperateUtil.batchUpdate(SQL,parameters);
                    }
                }
                else
                {
                    SleepUtil.sleepSecond(2);
                }
            }
        }
    }

//    @Pointcut("execution(* com.boulderai.metabase.sync.core.service.neo4j.Neo4jDataRepository.*(..))")
    public void logstat() {
    }



//    @Around("logstat()")
    public Object doAround(ProceedingJoinPoint joinPoint) throws Throwable {
        Object[] args = joinPoint.getArgs();
        int methodType=0;
        String  parameterStr=null;
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Class[] paramTypeArray = methodSignature.getParameterTypes();
        StringBuilder cqlBuilder = new StringBuilder();
        if (List.class.isAssignableFrom(paramTypeArray[0])) {
            List list= (List) args[0];
            for (Object cql:list) {
                cqlBuilder.append(cql).append(";    ");
            }
            methodType=0;
        }
       else  if (CqlValueContainer.class.isAssignableFrom(paramTypeArray[0])) {
            CqlValueContainer cqlContainer= (CqlValueContainer) args[0];
            String cql=cqlContainer.getCql();
            cqlBuilder.append(cql).append("  ###CqlValueContainer ");
//            LinkedList<Value> valueList=cqlContainer.getValueList();
//            for(Value  value:valueList )
//            {
//                cqlBuilder.append(value.getValue());
//            }
            methodType=1;

        }
       else  if (String.class.isAssignableFrom(paramTypeArray[0])) {
           String cql= (String) args[0];
            cqlBuilder.append(cql);
            methodType=2;
        }

//        log.info("请求参数为{}",args);
        long startTime=System.currentTimeMillis();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Object result = joinPoint.proceed(args);
        long endTime=System.currentTimeMillis();
        stopWatch.stop();
        long useTime=stopWatch.getTime();
        long ut=endTime-startTime;

//        System.out.println(useTime+" ======> "+ut);


        CqlStat  cqlStat=new CqlStat();
        cqlStat.setUseTime(useTime);
        cqlStat.setCql(cqlBuilder.toString());
        cqlStat.setMethod(  methodSignature.getMethod().getName());
        cqlStat.setMethodType(methodType);
        cqlStatQueue.add(cqlStat);

//        log.info("响应结果为{}",result);
        //如果这里不返回result，则目标对象实际返回值会被置为null
        return result;
    }

    public void addCqlStat(CqlStat  cqlStat)
    {
        cqlStatQueue.add(cqlStat);
    }

//
//    @Before("webLog()")
//    public void doBefore(JoinPoint joinPoint) {
//        // 接收到请求，记录请求内容
////        logger.info("WebLogAspect.doBefore()");
//        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
//        HttpServletRequest request = attributes.getRequest();
//
//        // 记录下请求内容
//        log.info("URL : " + request.getRequestURL().toString());
//        log.info("HTTP_METHOD : " + request.getMethod());
//        log.info("IP : " + request.getRemoteAddr());
//        log.info("CLASS_METHOD : " + joinPoint.getSignature().getDeclaringTypeName() + "." + joinPoint.getSignature().getName());
//        log.info("ARGS : " + Arrays.toString(joinPoint.getArgs()));
//        //获取所有参数方法一：
//        Enumeration<String> enu = request.getParameterNames();
//        while (enu.hasMoreElements()) {
//            String paraName = (String) enu.nextElement();
//            System.out.println(paraName + ": " + request.getParameter(paraName));
//        }
//    }




//    @Around("operNeo4jDataByCqlSingle()")
//    public void doAround(JoinPoint joinPoint) {
//        // 接收到请求，记录请求内容
////        logger.info("WebLogAspect.doBefore()");
//        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
//        HttpServletRequest request = attributes.getRequest();
//
//        // 记录下请求内容
//        log.info("URL : " + request.getRequestURL().toString());
//        log.info("HTTP_METHOD : " + request.getMethod());
//        log.info("IP : " + request.getRemoteAddr());
//        log.info("CLASS_METHOD : " + joinPoint.getSignature().getDeclaringTypeName() + "." + joinPoint.getSignature().getName());
//        log.info("ARGS : " + Arrays.toString(joinPoint.getArgs()));
//        //获取所有参数方法一：
//        Enumeration<String> enu = request.getParameterNames();
//        while (enu.hasMoreElements()) {
//            String paraName = (String) enu.nextElement();
//            System.out.println(paraName + ": " + request.getParameter(paraName));
//        }
//    }


//    @AfterReturning("webLog()")
//    public void doAfterReturning(JoinPoint joinPoint) {
//        // 处理完请求，返回内容
//        log.info("WebLogAspect.doAfterReturning()");
//    }


}
