package vt.lee.lab.storm.riot_resources;

import java.util.ArrayList;
import java.util.List;

public class RiotResourceFileProps {

	private static List<String> resourceFileProps = new ArrayList<String>();

	public static List<String> getRiotResourceFileProps() {

		resourceFileProps.add("PARSE.CSV_SCHEMA_FILEPATH");
		resourceFileProps.add("CLASSIFICATION.DECISION_TREE.ARFF_PATH");
		resourceFileProps.add("CLASSIFICATION.DECISION_TREE.MODEL_PATH");
		resourceFileProps.add("FILTER.BLOOM_FILTER.MODEL_PATH");
		resourceFileProps.add("FILTER.MULTI_BLOOM_FILTER.MODEL_PATH_LIST");
		resourceFileProps.add("PARSE.CSV_SCHEMA_WITH_ANNOTATEDFIELDS_FILEPATH");
		resourceFileProps.add("PREDICT.LINEAR_REGRESSION.TRAIN.ARFF_PATH");
		resourceFileProps.add("PREDICT.LINEAR_REGRESSION.MODEL_PATH");
		resourceFileProps.add("ANNOTATE.ANNOTATE_FILE_PATH");
		resourceFileProps.add("JOIN.SCHEMA_FILE_PATH");
		resourceFileProps.add("FILTER.BLOOM_FILTER.MODEL_PATH");

		return resourceFileProps;
	}

	public static List<String> getStatsTopoResourceFileProps() {

		resourceFileProps.add("PARSE.CSV_SCHEMA_FILEPATH");
		resourceFileProps.add("CLASSIFICATION.DECISION_TREE.ARFF_PATH");
		resourceFileProps.add("CLASSIFICATION.DECISION_TREE.MODEL_PATH");
		resourceFileProps.add("FILTER.BLOOM_FILTER.MODEL_PATH");
		resourceFileProps.add("FILTER.MULTI_BLOOM_FILTER.MODEL_PATH_LIST");
		resourceFileProps.add("PARSE.CSV_SCHEMA_WITH_ANNOTATEDFIELDS_FILEPATH");
		resourceFileProps.add("PREDICT.LINEAR_REGRESSION.TRAIN.ARFF_PATH");
		resourceFileProps.add("PREDICT.LINEAR_REGRESSION.MODEL_PATH");
		resourceFileProps.add("ANNOTATE.ANNOTATE_FILE_PATH");
		resourceFileProps.add("JOIN.SCHEMA_FILE_PATH");
		resourceFileProps.add("PREDICT.LINEAR_REGRESSION.SAMPLE_HEADER");
		
		resourceFileProps.add("FILTER.BLOOM_FILTER.MODEL_PATH");

		return resourceFileProps;
	}
	
	public static List<String> getPredTaxiResourceFileProps() {

		resourceFileProps.add("PARSE.CSV_SCHEMA_FILEPATH");
		resourceFileProps.add("CLASSIFICATION.DECISION_TREE.ARFF_PATH");
		resourceFileProps.add("CLASSIFICATION.DECISION_TREE.MODEL_PATH");
		resourceFileProps.add("FILTER.BLOOM_FILTER.MODEL_PATH");
		resourceFileProps.add("FILTER.MULTI_BLOOM_FILTER.MODEL_PATH_LIST");
		resourceFileProps.add("PARSE.CSV_SCHEMA_WITH_ANNOTATEDFIELDS_FILEPATH");
		resourceFileProps.add("PREDICT.LINEAR_REGRESSION.TRAIN.ARFF_PATH");
		resourceFileProps.add("PREDICT.LINEAR_REGRESSION.MODEL_PATH");
		resourceFileProps.add("ANNOTATE.ANNOTATE_FILE_PATH");
		resourceFileProps.add("JOIN.SCHEMA_FILE_PATH");
		
		resourceFileProps.add("FILTER.BLOOM_FILTER.MODEL_PATH");
		resourceFileProps.add("SPOUT.SENML_CSV_SCHEMA_PATH");
		resourceFileProps.add("PARSE.XML_FILEPATH");
		resourceFileProps.add("TRAIN.LINEAR_REGRESSION.MODEL_PATH");
		resourceFileProps.add("IO.AZURE_BLOB_UPLOAD.DIR_NAME");
		
		resourceFileProps.add("TRAIN.DECISION_TREE.MODEL_PATH");
		
		return resourceFileProps;
	}
	
	public static List<String> getPredSysResourceFileProps() {

		resourceFileProps.add("CLASSIFICATION.DECISION_TREE.ARFF_PATH");
		resourceFileProps.add("CLASSIFICATION.DECISION_TREE.MODEL_PATH");

		resourceFileProps.add("FILTER.BLOOM_FILTER.MODEL_PATH");
		resourceFileProps.add("FILTER.MULTI_BLOOM_FILTER.MODEL_PATH_LIST");
		resourceFileProps.add("FILTER.BLOOM_FILTER.MODEL_PATH");
		
		resourceFileProps.add("PARSE.CSV_SCHEMA_FILEPATH");
		resourceFileProps.add("PARSE.CSV_SCHEMA_WITH_ANNOTATEDFIELDS_FILEPATH");
		resourceFileProps.add("PARSE.XML_FILEPATH");
		
		resourceFileProps.add("PREDICT.LINEAR_REGRESSION.TRAIN.ARFF_PATH");
		resourceFileProps.add("PREDICT.LINEAR_REGRESSION.MODEL_PATH");
		
		resourceFileProps.add("TRAIN.LINEAR_REGRESSION.MODEL_PATH");
		resourceFileProps.add("TRAIN.DECISION_TREE.MODEL_PATH");

		resourceFileProps.add("ANNOTATE.ANNOTATE_FILE_PATH");
		
		resourceFileProps.add("JOIN.SCHEMA_FILE_PATH");
		
		resourceFileProps.add("SPOUT.SENML_CSV_SCHEMA_PATH");
		
		resourceFileProps.add("IO.AZURE_BLOB_UPLOAD.DIR_NAME");
		
		
		return resourceFileProps;
	}
	
}
