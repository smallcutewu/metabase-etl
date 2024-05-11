package com.boulderai.metabase.etl.tl.neo4j.service.parser;


import com.boulderai.metabase.lang.er.RelationType;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: RelationCqlParserFactory
 * @Description: 关系解析器工厂
 * @author  df.l
 * @date 2022年10月12日
 * @Copyright boulderaitech.com
 */

public class RelationCqlParserFactory {
    private final Map<RelationType, RelationCqlParser>  relationCqlParserMap=new HashMap<RelationType, RelationCqlParser>();

    private static  class Holder{
      final  static   RelationCqlParserFactory INSTANCE =new RelationCqlParserFactory();
    }

    private RelationCqlParserFactory(){init();}

    private void init()
    {
        RelationCqlParser  parser=new ErRelationCqlParser();
        relationCqlParserMap.put(parser.getRelationType(),parser);

        parser=new FerRelationCqlParser();
        relationCqlParserMap.put(parser.getRelationType(),parser);

        parser=new MborRelationCqlParser();
        relationCqlParserMap.put(parser.getRelationType(),parser);


        parser=new SbrRelationCqlParser();
        relationCqlParserMap.put(parser.getRelationType(),parser);

//        parser=new SerRelationCqlParser();
//        relationCqlParserMap.put(parser.getRelationType(),parser);

        parser=new SfmrRelationCqlParser();
        relationCqlParserMap.put(parser.getRelationType(),parser);


        parser=new SmbrRelationCqlParser();
        relationCqlParserMap.put(parser.getRelationType(),parser);

    }

    public static RelationCqlParserFactory getInstance() {
        return  Holder.INSTANCE;
    }

    public  RelationCqlParser getRelationCqlParser(RelationType relationType) {
        return  relationCqlParserMap.get(relationType);
    }

}
