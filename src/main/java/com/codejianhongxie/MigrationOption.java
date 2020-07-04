package com.codejianhongxie;

import com.codejianhongxie.util.Constants;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/26 10:25
 */
public class MigrationOption {

    private static Pattern pattern = Pattern.compile("[0-9]*");

    //数据库连接相关参数选项
    /**源端 SequoiaDB 地址*/
    private String hosts;
    /**源端 SequoiaDB 连接用户*/
    private String user;
    /**源端 SequoiaDB 连接用户密码*/
    private String password;
    /**源端集合 full 名*/
    private String collection;
    /**目标端 SequoiaDB 地址*/
    private String dstHosts;
    /**目标端 SequoiaDB 连接用户*/
    private String dstUser;
    /**目标端 SequoiaDB 连接用户密码*/
    private String dstPassword;
    /**目标端 SequoiaDB 集合 full 名*/
    private String dstCollection;
    /**密码加密类型, 支持: text,sm2*/
    private String encryptType;

    //通用参数选项
    /**数据迁移操作类型, 支持 reader,validate,repair */
    private String operation;
    private boolean isMigration = false;
    private boolean isValidate = false;
    private boolean isRepair = false;
    /**数据迁移数据类型, 支持 lob,data*/
    private String type;
    private boolean typeIsLob = false;
    private boolean typeIsData = false;
    /**写入线程数量*/
    private int jobs;
    /**进度通知间隔, 0 表示不显示进度*/
    private int reportInterval;
    /**批量插入的数量*/
    private int insertNum;
    /**是否自动查找协调节点*/
    private boolean coord;
    /**最大限速，lob对象是以MB为单位，结构化数据、半结构化数据是以条数为单位*/
    private int maxRate;

    //lob对象修复参数选项
    /** lob对象 id 文件*/
    private String fileName;

    /**
     * 迁移工具通用参数
     */
    private void generalOptions(Options options) {

        Option operation = new Option("o", "operation",true, "operation type[migration,validate,repair], data type of record only support migration");
        Option type = new Option("t", "type", true, "type of record, default: lob(lob,data)");
        Option threads = new Option("j", "jobs",  true, "number of jobs at once,default 1");
        Option reportInterval = new Option("i", "interval",  true, "periodically report intermediate statistics with a specified interval in seconds. 0 disables intermediate reports, default: 0");
        Option insertNum = new Option("n", "insertnum",  true, "the record count of each thread process, default 1000");
        Option coord = new Option("crd","coord", true, "find coordinators automatically, default: true");
        Option maxRate = new Option("r","rate",true, "the max rate[MB/s] of transfer data,(Specify it when ) 0 means unlimited, default:0");
        Option encrypt = new Option("e", "encrypt", true, "password encrypt type, default: text(text, sm2)");

        operation.setRequired(true);

        options.addOption(operation);
        options.addOption(type);
        options.addOption(threads);
        options.addOption(reportInterval);
        options.addOption(insertNum);
        options.addOption(coord);
        options.addOption(maxRate);
        options.addOption(encrypt);
    }

    /**
     * 数据库相关参数选项
     */
    private void databaseOptions(Options options) {


        Option hosts = new Option("h", "hosts ",  true, "source sequoiadb host,default \"localhost:11810\", multiple addresses should be separated by \",\"");
        Option user = new Option("u", "user ",  true, "source sequoiadb user,default \"\"");
        Option password = new Option("p", "password ",  true, "source sequoiadb password,default \"\"");
        Option collection = new Option("c", "collection",  true, "source sequoiadb full name of collection, eg:\"cs.cl\"");
        Option dstHosts = new Option("dsth", "dsthosts ",  true, "destination sequoiadb hosts,default \"localhost:11810\",multiple addresses should be separated by \",\"");
        Option dstUser = new Option("dstu", "dstuser ",  true, "destination sequoiadb user,default \"\"");
        Option dstPassword = new Option("dstp", "dstpassword ",  true, "destination sequoiadb password,default \"\"");
        Option dstCollection = new Option("dstc", "dstcollection",  true, "destination sequoiadb full name of collection, eg:\"cs.cl\"");


        collection.setRequired(true);
        dstCollection.setRequired(true);

        options.addOption(hosts);
        options.addOption(user);
        options.addOption(password);
        options.addOption(collection);
        options.addOption(dstHosts);
        options.addOption(dstUser);
        options.addOption(dstPassword);
        options.addOption(dstCollection);
    }

    /**
     * lob对象修复相关参数
     */
    private void repairOptions(Options options) {

        Option fileName = new Option("f","file", true, "input files name(Specify it when operation use repair)");
        options.addOption(fileName);
    }

    /**
     * 获取迁移工具的参数选项
     * @return
     */
    public Options getMigrationOptions() {

        Options options = new Options();
        generalOptions(options);
        databaseOptions(options);
        repairOptions(options);
        return options;
    }

    public void printUsage() {

        Options options = getMigrationOptions();
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(" [options] ", options);
    }

    /**
     * 从命令行参数中解析参数选项
     * @param line
     */
    public void parseOptions(CommandLine line) {

        if (line.hasOption("h")) {
            setHosts(line.getOptionValue("h"));
        } else {
            setHosts("localhost:11810");
        }
        if (line.hasOption("u")) {
            setUser(line.getOptionValue("u"));
        } else {
            setUser("");
        }
        if (line.hasOption("p")) {
            setPassword(line.getOptionValue("p"));
        } else {
            setPassword("");
        }
        if (line.hasOption("c")) {
            setCollection(line.getOptionValue("c"));
        }
        if (line.hasOption("dsth")) {
            setDstHosts(line.getOptionValue("dsth"));
        } else {
            setDstHosts("localhost:11810");
        }
        if (line.hasOption("dstu")) {
            setDstUser(line.getOptionValue("dstu"));
        } else {
            setDstUser("");
        }
        if (line.hasOption("dstp")) {
            setDstPassword(line.getOptionValue("dstp"));
        } else {
            setDstPassword("");
        }
        if (line.hasOption("dstc")) {
            setDstCollection(line.getOptionValue("dstc"));
        }
        if (line.hasOption("o")) {
            String operation = line.getOptionValue("o");
            if (Constants.DEFAULT_MIGRATION_TYPE.equalsIgnoreCase(operation)) {
                setMigration(true);
            } else if (Constants.DEFAULT_VALIDATE_TYPE.equalsIgnoreCase(operation)) {
                setValidate(true);
            } else if (Constants.DEFAULT_REPAIR_TYPE.equalsIgnoreCase(operation)) {
                setRepair(true);
            } else {
                throw new IllegalArgumentException("the value of operation must be migration or validate or repair");
            }
            setOperation(operation);
        }
        if (line.hasOption("t")) {
            String type = line.getOptionValue("t");
            if ("lob".equalsIgnoreCase(type)) {
                setTypeIsLob(true);
            } else if ("data".equalsIgnoreCase(type)) {
                setTypeIsData(true);
            } else {
                throw new IllegalArgumentException("the value of type must be lob or data");
            }
            setType(type);
        } else {
            setType("lob");
            setTypeIsLob(true);
        }

        if (line.hasOption("j")) {
            String jobs = line.getOptionValue("j");
            Matcher isNum = pattern.matcher(jobs);
            if( !isNum.matches() ){
                throw new IllegalArgumentException("the value of jobs must be a number");
            }
            setJobs(Integer.valueOf(jobs));
        } else {
            setJobs(1);
        }
        if (line.hasOption("i")) {
            String reportInterval = line.getOptionValue("i");
            Matcher isNum = pattern.matcher(reportInterval);
            if ( !isNum.matches()) {
                throw new IllegalArgumentException("the value of report interval must be a number");
            }
            setReportInterval(Integer.valueOf(reportInterval));
        } else {
            setReportInterval(0);
        }
        if (line.hasOption("n")) {
            String insertNum = line.getOptionValue("n");
            Matcher isNum = pattern.matcher(insertNum);
            if (!isNum.matches()) {
                throw new IllegalArgumentException("the value of insertnum must be a number");
            }
            setInsertNum(Integer.valueOf(insertNum));
        } else {
            setInsertNum(1000);
        }
        if (line.hasOption("crd")) {
            String coord = line.getOptionValue("crd");
            if ("true".equalsIgnoreCase(coord)
                    || "false".equalsIgnoreCase(coord)) {
                setCoord(Boolean.valueOf(coord));
            } else {
                throw new IllegalArgumentException("the value of coord must be true or false");
            }
        } else {
            setCoord(true);
        }
        if (line.hasOption("r")) {
            String rate = line.getOptionValue("r");
            Matcher isNum = pattern.matcher(rate);
            if (!isNum.matches()) {
                throw new IllegalArgumentException("the value of rate must be a number");
            }
            int rateValue = Integer.valueOf(rate);
            if (rateValue < 0 ) {
                rateValue = 0;
            } else {
                rateValue = rateValue * 1024;
            }
            setMaxRate(rateValue);
        } else {
            setMaxRate(0);
        }
        if (line.hasOption("e")) {
            String encrypt = line.getOptionValue("e");
            if ("text".equalsIgnoreCase(encrypt)
                    || "sm2".equalsIgnoreCase(encrypt)) {
                setEncryptType(encrypt);
            } else {
                throw new IllegalArgumentException("the value of encrypt must be text or sm2");
            }
        } else {
            setEncryptType("text");
        }
        if (line.hasOption("f")) {
            String fileName = line.getOptionValue("f");
            File repairFile = new File(fileName);
            if (!repairFile.exists()) {
                throw new IllegalArgumentException(fileName + " is not exists");
            }
            setFileName(fileName);
        }
    }

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getDstHosts() {
        return dstHosts;
    }

    public void setDstHosts(String dstHosts) {
        this.dstHosts = dstHosts;
    }

    public String getDstUser() {
        return dstUser;
    }

    public void setDstUser(String dstUser) {
        this.dstUser = dstUser;
    }

    public String getDstPassword() {
        return dstPassword;
    }

    public void setDstPassword(String dstPassword) {
        this.dstPassword = dstPassword;
    }

    public String getDstCollection() {
        return dstCollection;
    }

    public void setDstCollection(String dstCollection) {
        this.dstCollection = dstCollection;
    }

    public String getEncryptType() {
        return encryptType;
    }

    public void setEncryptType(String encryptType) {
        this.encryptType = encryptType;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public int getJobs() {
        return jobs;
    }

    public void setJobs(int jobs) {
        this.jobs = jobs;
    }

    public int getReportInterval() {
        return reportInterval;
    }

    public void setReportInterval(int reportInterval) {
        this.reportInterval = reportInterval;
    }

    public int getInsertNum() {
        return insertNum;
    }

    public void setInsertNum(int insertNum) {
        this.insertNum = insertNum;
    }

    public boolean isCoord() {
        return coord;
    }

    public void setCoord(boolean coord) {
        this.coord = coord;
    }

    public int getMaxRate() {
        return maxRate;
    }

    public void setMaxRate(int maxRate) {
        this.maxRate = maxRate;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public static Pattern getPattern() {
        return pattern;
    }

    public static void setPattern(Pattern pattern) {
        MigrationOption.pattern = pattern;
    }

    public boolean isMigration() {
        return isMigration;
    }

    public void setMigration(boolean migration) {
        isMigration = migration;
    }

    public boolean isValidate() {
        return isValidate;
    }

    public void setValidate(boolean validate) {
        isValidate = validate;
    }

    public boolean isRepair() {
        return isRepair;
    }

    public void setRepair(boolean repair) {
        isRepair = repair;
    }

    public boolean isTypeIsLob() {
        return typeIsLob;
    }

    public void setTypeIsLob(boolean typeIsLob) {
        this.typeIsLob = typeIsLob;
    }

    public boolean isTypeIsData() {
        return typeIsData;
    }

    public void setTypeIsData(boolean typeIsData) {
        this.typeIsData = typeIsData;
    }


}
