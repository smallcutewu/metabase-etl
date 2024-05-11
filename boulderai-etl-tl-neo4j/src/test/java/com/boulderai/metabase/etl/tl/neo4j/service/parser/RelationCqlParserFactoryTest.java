package com.boulderai.metabase.etl.tl.neo4j.service.parser;

import com.boulderai.metabase.lang.er.RelationType;
import com.boulderai.metabase.etl.tl.neo4j.SpringBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class RelationCqlParserFactoryTest extends SpringBaseTest {

    @Test
    public void testGetRelationCqlParser()
    {
        RelationCqlParserFactory  factory= RelationCqlParserFactory.getInstance();
        RelationCqlParser  relationCqlParser= factory.getRelationCqlParser(RelationType.ER);
        Assert.assertNotNull(relationCqlParser);
    }

}
