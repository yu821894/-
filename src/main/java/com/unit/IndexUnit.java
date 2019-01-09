package com.unit;

import com.mongoconnect.MongoConnect;
import com.mongodb.client.MongoCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class IndexUnit {

    private final static String INDEX_DIR = "D:\\idea\\indexplace";
    private static Directory dir ;
    private static IndexWriter indexWriter;
    private static IndexReader indexReader;
    private static IndexSearcher indexSearcher;
    public static Map<String,String> queryOption = new HashMap<>();
    private static Logger logger = LogManager.getLogger(IndexUnit.class);



    static {
        try {
            dir= FSDirectory.open(new File(INDEX_DIR));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void createAllIndex(){


        try {
            int storeCount = 0;

            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_45,new StandardAnalyzer(Version.LUCENE_45));
            indexWriterConfig.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());

            indexWriter = new IndexWriter(dir,indexWriterConfig);


            //连接数据库获得所有数据
            long ms = System.currentTimeMillis();
            MongoCursor<Document> cursor = MongoConnect.getMongoCursor();
            long me = System.currentTimeMillis();

            org.apache.lucene.document.Document document = new org.apache.lucene.document.Document();

            if (!cursor.hasNext()){
                logger.info("没有查到数据");
                return;
            }
            int option = 10000;
            long ls = System.currentTimeMillis();
            while (cursor.hasNext()){
                Document dbDoc = cursor.next();
                //添加索引，文档id储存
                document.add(new TextField("_id",dbDoc.get("_id").toString() , Field.Store.YES));

                //索引的净重储存
                document.add(new TextField("netWeight", dbDoc.get("netWeight") == null?"0":dbDoc.get("netWeight").toString(), Field.Store.YES));
                //索引的文档创建的时间储存
                document.add(new TextField("grade", dbDoc.get("grade") == null?"nothing":dbDoc.get("grade").toString(), Field.Store.YES));
                //打包丝锭数量
                document.add(new TextField("silkCount",dbDoc.get("silkCount").toString() , Field.Store.YES));
                //打包时间
                //document.add(new NumericDocValuesField("cdt",((Date)(dbDoc.get("cdt"))).getTime()));
                document.add(new LongField("cdt",((Date)dbDoc.get("cdt")).getTime(),Field.Store.YES));
                //公共标志
                //document.add(new TextField("content", "index", Field.Store.YES));

                indexWriter.addDocument(document);
                indexWriter.commit();

                storeCount += 1;
                document.removeField("_id");
                document.removeField("netWeight");
                document.removeField("grade");
                document.removeField("silkCount");
                document.removeField("cdt");
                //document.removeField("content");
                if (storeCount == option){
                    logger.info("正在制作索引："+storeCount);
                    option += 10000;
                }


            }
            long le = System.currentTimeMillis();
            logger.info("====================================================");
            logger.info("索引制作完成 索引数量："+storeCount+" 耗时:"+(le-ls)+" dbCost: "+(me-ms));
            logger.info("====================================================");



        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("提交失败");
        }finally {
            IndexUnit.writerClose();
        }
    }

    //更新索引
    public static void updateIndex(){

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //连接数据库获得所有数据
        long dst = System.currentTimeMillis();
        MongoCursor<Document> cursor = MongoConnect.getMongoCursor();
        long det = System.currentTimeMillis();
        logger.info("更新步骤dbCost: "+(det-dst));

        try {

            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_45,new StandardAnalyzer(Version.LUCENE_45));
            indexWriterConfig.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());

            indexWriter = new IndexWriter(dir,indexWriterConfig);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (!cursor.hasNext()){
            System.out.println("无数据");
            return ;
        }
        int countNew = 0;
        List<org.apache.lucene.document.Document> documents = new ArrayList<>();
        long us = System.currentTimeMillis();
        while (cursor.hasNext()){

            Document docObject = cursor.next();
            if (!isIdInIndex(docObject.get("_id").toString())){
                org.apache.lucene.document.Document document = new org.apache.lucene.document.Document();
                //添加索引，文档id储存
                document.add(new TextField("_id",docObject.get("_id").toString() , Field.Store.YES));

                //索引的净重储存
                document.add(new TextField("netWeight", docObject.get("netWeight") == null?"0":docObject.get("netWeight").toString(), Field.Store.YES));
                //索引的文档创建的时间储存
                document.add(new TextField("grade", docObject.get("grade") == null?"nothing":docObject.get("grade").toString(), Field.Store.YES));
                //打包丝锭数量
                document.add(new TextField("silkCount",docObject.get("silkCount").toString() , Field.Store.YES));
                //打包时间
                document.add(new LongField("cdt",((Date)docObject.get("cdt")).getTime(),Field.Store.YES));
                //公共标志
                //document.add(new TextField("content", "index", Field.Store.YES));
                documents.add(document);
                countNew++;
            }
        }
        try {
            if (documents.isEmpty()){
                logger.info("无数据更新");
                return;
            }
            indexWriter.addDocuments(documents);
            indexWriter.commit();
            long ue = System.currentTimeMillis();
            logger.info(sdf.format(new Date())+": 新增数据"+countNew+"条 耗时: "+(ue-us));
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IndexUnit.writerClose();
        }
    }

    //索引是否包含此id
    public static boolean isIdInIndex(String id){
        queryOption.put("_id",id);
        if (IndexUnit.searchIndex(queryOption).size() == 0){
            return false;
        }
        return true;

    }

    //查找索引
    public static List<org.apache.lucene.document.Document> searchIndex(Map<String,String> queryOption){

        long sst = System.currentTimeMillis();
        List<org.apache.lucene.document.Document> documents = new ArrayList<>();
        try {

            indexReader = IndexReader.open(dir);
            indexSearcher = new IndexSearcher(indexReader);

            BooleanQuery booleanQuery = new BooleanQuery();
            long startTime = 0l;
            long endTime = 0l;
            for (Map.Entry<String,String> entry : queryOption.entrySet()){
                if ("startTime".equals(entry.getKey()) || "endTime".equals(entry.getKey())){
                    if ("startTime".equals(entry.getKey())){
                        startTime = Long.parseLong(entry.getValue());
                    }else {
                        endTime = Long.parseLong(entry.getValue());
                    }
                    continue;
                }

                QueryParser parser =
                        new QueryParser(Version.LUCENE_45,entry.getKey(),new StandardAnalyzer(Version.LUCENE_45));
                Query query = parser.parse(entry.getValue());
                booleanQuery.add(query, BooleanClause.Occur.MUST);
            }

            if (startTime != 0 && endTime != 0){
                //加入日期范围查询
                NumericRangeQuery<Long> rangeQuery =
                        NumericRangeQuery.newLongRange("cdt",startTime,endTime,true,true);
                booleanQuery.add(rangeQuery, BooleanClause.Occur.MUST);
            }
            TopDocs docs = indexSearcher.search(booleanQuery,10000000);
            for (ScoreDoc result : docs.scoreDocs){
                org.apache.lucene.document.Document document = indexSearcher.doc(result.doc);
                documents.add(document);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }finally {
            IndexUnit.readerClose();
        }

        //清空map
        queryOption.clear();


        return documents;
    }

    //写资源关闭
    public static void writerClose(){
        try {
            if (indexWriter != null){
                indexWriter.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //读资源关闭
    public static void readerClose(){
        try {

            if (indexReader != null){
                indexReader.close();
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
