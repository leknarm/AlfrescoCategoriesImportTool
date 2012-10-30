package th.co.osdev;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.alfresco.webservice.repository.QueryResult;
import org.alfresco.webservice.repository.RepositoryFault;
import org.alfresco.webservice.types.CML;
import org.alfresco.webservice.types.CMLCreate;
import org.alfresco.webservice.types.NamedValue;
import org.alfresco.webservice.types.ParentReference;
import org.alfresco.webservice.types.Query;
import org.alfresco.webservice.types.Reference;
import org.alfresco.webservice.types.ResultSet;
import org.alfresco.webservice.types.ResultSetRow;
import org.alfresco.webservice.types.Store;
import org.alfresco.webservice.util.AuthenticationUtils;
import org.alfresco.webservice.util.Constants;
import org.alfresco.webservice.util.ISO9075;
import org.alfresco.webservice.util.Utils;
import org.alfresco.webservice.util.WebServiceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ImportCategories {

	public static final String ALFRESCO_USERNAME_PROPERTY = "repository.username";
	public static final String ALFRESCO_PASSWORD_PROPERTY = "repository.password";
	public static final String ALFRESCO_WEBSERVICECLIENT_PROPERTIES = "/alfresco/webserviceclient.properties";
	private static final String ROOT_CATEGORY = "/cm:generalclassifiable";
	private final String SUBCATEGORIES = "subcategories"; // the propertyname of // subcategories
	private final String SUBCATEGORIES_Q = Constants.createQNameString(Constants.NAMESPACE_CONTENT_MODEL, SUBCATEGORIES);
	private final String CATEGORY = "category"; // the propertyname of // subcategories
	private final String CATEGORY_Q = Constants.createQNameString(Constants.NAMESPACE_CONTENT_MODEL, CATEGORY);
	private final Store STORE = new Store(Constants.WORKSPACE_STORE, "SpacesStore");
	
	private int success = 0;
	/**
	 * Maps Parent Category Paths to Reference objects. Caches Parent Category
	 * lookups.
	 **/
	private Map<String, Reference> ROOT_REFERENCE_CACHE = new HashMap<String, Reference>();
	private final Log logger = LogFactory.getLog(getClass());

	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			printUsage();
		}
		
		ImportCategories migration = new ImportCategories();
		migration.runApp(args[0]);
	}

	private void runApp(String filename) throws IOException {
		logger.info("Importing categories from " + filename);
		setUp();
		Collection<String> categories = readCategoriesFromFile(filename);
		createCategories(categories);
		tearDown();
		logger.info(success + " Categories import completed");

	}

	private void tearDown() {
		AuthenticationUtils.endSession();
	}

	private void setUp() throws IOException {
		// Set the Default values.
		String username = "admin";
	    String password = "admin";
	      
	    // Attempt to read the username and password from the Alfresco web service client properties file.
	    InputStream is = getClass().getResourceAsStream(ALFRESCO_WEBSERVICECLIENT_PROPERTIES);
	    if(is!=null)
	    {
	       Properties properties = new Properties();
	       properties.load(is);
	       username = getUsername(properties, username);
	       password = getPassword(properties, password);
	    }
	      
	    // Attempt to read the username and password from the System properties.
	    username = getUsername(System.getProperties(), username);
	    password = getPassword(System.getProperties(), password);
	      
	    logger.info("creating Alfresco web service client session with username '"+username+"' and password '"+password+"'.");
		AuthenticationUtils.startSession(username, password);
	}
	
	protected String getUsername(Properties p, String originalValue) {
		if(p.containsKey(ALFRESCO_USERNAME_PROPERTY))
			return p.getProperty(ALFRESCO_USERNAME_PROPERTY);
		return originalValue;
	}
	   
	protected String getPassword(Properties p, String originalValue) {
		if(p.containsKey(ALFRESCO_PASSWORD_PROPERTY))
			return p.getProperty(ALFRESCO_PASSWORD_PROPERTY);
		return originalValue;
	}

	private void createCategories(Collection<String> categories) throws RepositoryFault, RemoteException {
		for (String category : categories)
			createCategory(category);

	}

	private void createCategory(String path) throws RepositoryFault, RemoteException {
		// Recursively create parent categories, if necessary.
		Reference rootRef = null;
		String parentCategoryPath = getParentCategoryPath(path);
		while ((rootRef = getRootReference(parentCategoryPath)) == null) {
			createCategory(parentCategoryPath);
		}

		// Make sure the category doesn't already exist in Alfresco.
		if (getCategory(path) != null) {
			logger.info("category already exists: " + path);
			return;
		}

		// Create the category.
		logger.info("creating category:" + path);
		String childCategoryName = getChildCategoryName(path);
		ParentReference parentRef = new ParentReference(STORE, rootRef.getUuid(), null, SUBCATEGORIES_Q, Constants.createQNameString(Constants.NAMESPACE_CONTENT_MODEL, childCategoryName));
		createCategory(childCategoryName, parentRef);
		success++;
	}

	private Reference getRootReference(String parentCategoryPath) throws RepositoryFault, RemoteException {
		// Return Cached Reference, if available.
		if (ROOT_REFERENCE_CACHE.containsKey(parentCategoryPath))
			return ROOT_REFERENCE_CACHE.get(parentCategoryPath);

		// Get the Category from Alfresco, cache it, and return it.
		Reference r = getCategory(parentCategoryPath);
		if (r != null)
			ROOT_REFERENCE_CACHE.put(parentCategoryPath, r); // Add Reference to cache.
		return r;
	}

	private Reference getCategory(String categoryPath) throws RepositoryFault, RemoteException {
		String luceneQueryString = "PATH:\"" + ROOT_CATEGORY + (categoryPath == null ? "" : encodeCategoryPath(categoryPath)) + "\"";
		Query query = new Query(Constants.QUERY_LANG_LUCENE, luceneQueryString);

		QueryResult result = WebServiceFactory.getRepositoryService().query(STORE, query, true);
		ResultSet rs = result.getResultSet();
		if (rs.getTotalRowCount() == 0)
			return null;
		ResultSetRow[] rows = rs.getRows();
		String uuid = rows[0].getNode().getId();
		return new Reference(STORE, uuid, null);
	}

	public static final String encodeCategoryPath(String categoryPath) {
		categoryPath = categoryPath.substring(4); // Strip off the leading
													// '/cm:'
		String[] categories = categoryPath.split("/cm:"); // Split the category
															// path by the
															// remaining '/cm:'
															// strings.
		StringBuffer encodedCategoryPath = new StringBuffer();
		for (String category : categories) {
			encodedCategoryPath.append("/cm:");
			encodedCategoryPath.append(ISO9075.encode(category));
		}
		return encodedCategoryPath.toString();
	}

	private void createCategory(String categoryName, ParentReference parentRef) throws RepositoryFault, RemoteException {
		NamedValue[] properties = new NamedValue[] { Utils.createNamedValue(Constants.PROP_NAME, categoryName) };

		CMLCreate create = new CMLCreate("1", parentRef, null, null, null, CATEGORY_Q, properties);
		CML cml = new CML();
		cml.setCreate(new CMLCreate[] { create });

		WebServiceFactory.getRepositoryService().update(cml);
	}

	private String getChildCategoryName(String path) {
		return path.substring(path.lastIndexOf("/cm:") + 4); // get the last category and strip off the "/cm:".
	}

	private String getParentCategoryPath(String path) {
		if (path.lastIndexOf("/") <= 0)
			return null;

		return path.substring(0, path.lastIndexOf("/"));
	}

	private Collection<String> readCategoriesFromFile(String filename) throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(filename));
		Vector<String> categories = new Vector<String>();
		String str = null;
		while ((str = in.readLine()) != null) {
			if (str.trim().length() == 0)
				continue; // skip blank lines
			if (str.trim().startsWith("#"))
				continue; // skip comments starting with the # character
			categories.add(str);
		}
		in.close();
		return categories;
	}

	private static void printUsage() {
		System.out.println("java " + ImportCategories.class.getName() + " <filename>");
	}

}
