package com.ttajun.es;

import com.ttajun.es.conn.EsHighRestConnector;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App 
{
    private final static Logger log = LoggerFactory.getLogger(App.class);
    public static void main( String[] args )
    {
        log.info("elasticsearch client");
        EsHighRestConnector esHighRestConnector = new EsHighRestConnector();

        Options options = new Options();
        options.addOption("h", "help", false,  "shows this help message");

        // command
        options.addOption("fs", "fields", false, "print sensor's field list. -i <ktme|globiz> ");
        options.addOption("ts", "trains", false, "print train list. -i <ktme|globiz> ");
        options.addOption("cs", "cars", false, "print car list. -i <ktme|globiz> -t <train id>");
        options.addOption("ps", "parts", false, "print part list. -i <ktme|globiz> -t <train id> -c <car id>");
        options.addOption("g", "get", false, "print data. " +
                "-i <ktme|globiz> -t <train id> -c <car id> -p <part type> -s <start time> -e <end time>");
        options.addOption("u", "update", false, "update data. " +
                "-i <ktme|globiz> -t <train id> -c <car id> -p <part type> -s <start time> -e <end time> -d <value>");

        // argument
        options.addOption("i", "index", true, "ktme or globiz");
        options.addOption("t", "train", true, "train id");
        options.addOption("c", "car", true, "car id");
        options.addOption("p", "part", true, "part type");
        options.addOption("s", "start", true, "start time. yy-mm-dd");
        options.addOption("e", "end", true, "end time. yy-mm-dd");
        options.addOption("d", "data", true, "u_defect_prob value. (20|30|40)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmdLine = null;

        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            usage(options);
            System.exit(0);
        }

        if(cmdLine.hasOption("help")) {
            usage(options);
            System.exit(0);
        } else if (cmdLine.hasOption("fields")) {
            if(cmdLine.hasOption("index")) {
                String fieldList = esHighRestConnector.getFieldList(cmdLine.getOptionValue("index"));

                if(!fieldList.equals("")) {
                    log.info(fieldList);
                    System.exit(0);
                }
            }
        } else if (cmdLine.hasOption("trains")) {
            if(cmdLine.hasOption("index")) {
                String trainList = esHighRestConnector.getTrainList(cmdLine.getOptionValue("index"));

                if(!trainList.equals("")) {
                    log.info(trainList);
                    System.exit(0);
                }
            }
        } else if (cmdLine.hasOption("cars")) {
            if(cmdLine.hasOption("index") && cmdLine.hasOption("train")) {
                String carList = esHighRestConnector.getCarList(cmdLine.getOptionValue("index"), cmdLine.getOptionValue("train"));

                if(!carList.equals("")) {
                    log.info(carList);
                    System.exit(0);
                }
            }
        } else if (cmdLine.hasOption("parts")) {
            String index = cmdLine.getOptionValue("index");
            String train = cmdLine.getOptionValue("train");
            String car = cmdLine.getOptionValue("car");

            if(cmdLine.hasOption("index") && cmdLine.hasOption("train") && cmdLine.hasOption("car")) {
                String partList = esHighRestConnector.getPartList(index, train, car);

                if(!partList.equals("")) {
                    log.info(partList);
                    System.exit(0);
                }
            }
        } else if (cmdLine.hasOption("get")) {
            if(cmdLine.hasOption("index") && cmdLine.hasOption("train") && cmdLine.hasOption("car")
                    && cmdLine.hasOption("part") && cmdLine.hasOption("start") && cmdLine.hasOption("end")) {
                String index = cmdLine.getOptionValue("index");
                String train = cmdLine.getOptionValue("train");
                String car = cmdLine.getOptionValue("car");
                String part = cmdLine.getOptionValue("part");
                String start = cmdLine.getOptionValue("start");
                String end = cmdLine.getOptionValue("end");

                int count = 0;
                List<Map<String, Object>> data = esHighRestConnector.getData(index, train, car, part, start, end);
                log.info("total document: " + data.size());
                for(Map<String, Object> map : data) {
                    if(count++ > 5) break;;
                    for(String str : map.keySet()) {
                        log.info(str + " : " + map.get(str));
                    }
                    log.info("");
                }
                System.exit(0);
            }
        } else if(cmdLine.hasOption("update")) {
            if(cmdLine.hasOption("index") && cmdLine.hasOption("train") && cmdLine.hasOption("car")
                    && cmdLine.hasOption("part") && cmdLine.hasOption("start") && cmdLine.hasOption("end")
                    && cmdLine.hasOption("data")) {
                String index = cmdLine.getOptionValue("index");
                String train = cmdLine.getOptionValue("train");
                String car = cmdLine.getOptionValue("car");
                String part = cmdLine.getOptionValue("part");
                String start = cmdLine.getOptionValue("start");
                String end = cmdLine.getOptionValue("end");
                String data = cmdLine.getOptionValue("data");

                List<Map<String, Object>> esData = esHighRestConnector.getData(index, train, car, part, start, end);
                Map<String, String> updateData = new HashMap<>();
                log.info("total document: " + esData.size());
                for(Map<String, Object> map : esData) {
                    updateData.put((String)map.get("docID"), data);
                }

                esHighRestConnector.updateData(index, updateData);
            }
            System.exit(0);

        }

        usage(options);
        System.exit(0);
    }

    private static void usage(Options options) {
        HelpFormatter hf = new HelpFormatter();
        String runProgram = "java " + App.class.getName() + " [options]";
        hf.printHelp(runProgram, options);
    }
}
