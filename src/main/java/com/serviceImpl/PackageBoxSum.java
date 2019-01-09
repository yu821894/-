package com.serviceImpl;



import com.domain.Weight;
import com.service.ShowPackageBoxSum;
import com.unit.IndexUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;


public class PackageBoxSum implements ShowPackageBoxSum {


    static Logger logger = LogManager.getLogger(PackageBoxSum.class);

    public Weight show_PackageBoxSum(String startDate, String endDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startDateMS =0l;
        long endDateMS = 0l;
        try {
            startDateMS = sdf.parse(startDate).getTime();
            endDateMS = sdf.parse(endDate).getTime();
            if ((endDateMS - startDateMS) <= 0){
                Weight w = new Weight();
                w.setDoubleARate("0");
                return w;
            }
        } catch (ParseException e) {
            e.printStackTrace();
            logger.info("时间格式解析失败");
        }
        //指定时间内的所有记录数量
        int allSilks;
        //aa级记录数
        float aaSilks;
        //双a率
        float rate = 0;
        DecimalFormat df = new DecimalFormat("0.00000");

        Weight w = PackageBoxSum.getNetWeightAndCount(startDateMS+"",endDateMS+"");

        allSilks = w.getAllSilkCount();
        aaSilks = PackageBoxSum.getDoubleA(startDateMS+"",endDateMS+"");
        rate = aaSilks/allSilks;

        w.setDoubleARate(df.format(rate));
        return w;
    }

    public static Weight getNetWeightAndCount(String startDate, String endDate) {
        IndexUnit.queryOption.put("startTime",startDate);
        IndexUnit.queryOption.put("endTime",endDate);
        double netWeight = 0.0;
        int allSilkCount = 0;


        logger.info("====================================================");
        long st = System.currentTimeMillis();
        List<Document> documents = IndexUnit.searchIndex(IndexUnit.queryOption);
        long et = System.currentTimeMillis();
        logger.info("总打包数："+documents.size()+"包 耗时："+(et-st));
        logger.info("====================================================");

        for (Document result : documents){

            netWeight += Double.parseDouble(result.get("netWeight"));
            allSilkCount += Integer.parseInt(result.get("silkCount"));

        }
        Weight w = new Weight();
        w.setWeight(netWeight);
        w.setPackageCount(documents.size());
        w.setAllSilkCount(allSilkCount);

        return w;
    }

    public static float getDoubleA(String startDate, String endDate) {
        IndexUnit.queryOption.put("startTime",startDate);
        IndexUnit.queryOption.put("endTime",endDate);

        IndexUnit.queryOption.put("grade","1770980569354600486");


        logger.info("====================================================");
        long st = System.currentTimeMillis();
        List<Document> aaDocuments = IndexUnit.searchIndex(IndexUnit.queryOption);
        long et = System.currentTimeMillis();
        logger.info("aa级品打包数:"+aaDocuments.size()+"包 耗时："+(et-st));
        logger.info("====================================================");

        float count = 0f;
        float aaCount = 0f;
        for (Document d : aaDocuments){
            aaCount += Float.parseFloat(d.get("silkCount"));
        }


        IndexUnit.queryOption.put("startTime",startDate);
        IndexUnit.queryOption.put("endTime",endDate);
        IndexUnit.queryOption.put("grade","1788876173229424672");

        logger.info("====================================================");
        long saaat = System.currentTimeMillis();
        List<Document> aaaDocuments = IndexUnit.searchIndex(IndexUnit.queryOption);
        long eaaat = System.currentTimeMillis();
        logger.info("aaa级品打包数:"+aaaDocuments.size()+"包 耗时："+(eaaat-saaat));
        logger.info("====================================================");
        float aaaCount = 0f;
        for (Document d : aaaDocuments){
            aaaCount += Float.parseFloat(d.get("silkCount"));
        }
        count = aaCount + aaaCount;
        return count;
    }
}
