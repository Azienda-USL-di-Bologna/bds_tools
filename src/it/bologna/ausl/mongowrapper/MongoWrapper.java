package it.bologna.ausl.mongowrapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.bson.types.ObjectId;

import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;


public class MongoWrapper {
	private static Mongo m=null;
	private DB db;
	private GridFS gfs;
	public MongoWrapper(String host,Integer port, String db,String username,String password) throws UnknownHostException, MongoException, MongoWrapperException{
		if (m==null)
		{
			if (port != null)	m=new Mongo(host,port);
			else m=new Mongo(host);
			this.db=m.getDB(db);
			if (username!=null && password != null && this.db.isAuthenticated()!=true){
				 boolean auth = this.db.authenticate(username, password.toCharArray());
				 if (auth!=true)
					 throw new MongoWrapperException("Auth failed");
			}
		}
		this.db=m.getDB(db);
		gfs=new GridFS(this.db);
	}
	
	public MongoWrapper(String uri) throws UnknownHostException, MongoException, MongoWrapperException{
		MongoURI u=new MongoURI(uri);
		if (m==null)
		{
			m=new Mongo(u);
			this.db=m.getDB(u.getDatabase());
			if(u.getUsername()!=null){
				boolean auth = this.db.authenticate(u.getUsername(), u.getPassword());
				if (auth!=true)
					 throw new MongoWrapperException("Auth failed");
			}
		}
		this.db=m.getDB(u.getDatabase());
		
		gfs=new GridFS(this.db);
	}
	
	public boolean createDir(String dirname){
		
		DBCollection dirs=db.getCollection("dirs");
		String root="";
		boolean res=false;
		ObjectId o=null;
		for (String p : dirname.split("/") )
		{
			if (p.equals("")) continue;
			root+="/"+p;
			BasicDBObject d = new BasicDBObject().append("dirname",root);
		try{
			dirs.insert(d,com.mongodb.WriteConcern.JOURNAL_SAFE);
			o=(ObjectId) d.get("_id");
			res=true;
		}
		catch (com.mongodb.MongoException.DuplicateKey e){
			
		}
		}
		return res;
	}
	
	public String put(InputStream f,String filename,String dirname) throws IOException{
		GridFSInputFile inf=null;
		deletebyPath(dirname+'/'+filename);
		inf = gfs.createFile(f);
		inf.setFilename(dirname+'/'+filename);
		inf.save();
		f.close();
		createDir(dirname);
		return ObjectId.massageToObjectId(inf.getId()).toString();
	} 
	
	
	public String put(File f,String filename,String dirname) throws IOException{
		GridFSInputFile inf=null;
		deletebyPath(dirname+'/'+filename);
		inf = gfs.createFile(f);
		inf.setFilename(dirname+'/'+filename);
		inf.save();
		createDir(dirname);
		return ObjectId.massageToObjectId(inf.getId()).toString();
	}
	
	
	public String put(InputStream f,String id ,String filename,String dirname) throws IOException{
		GridFSInputFile inf=null;
		deletebyPath(dirname+'/'+filename);
		inf = gfs.createFile(f);
		inf.setFilename(dirname+'/'+filename);
		inf.setId(id);
		inf.save();
		f.close();
		createDir(dirname);
		return id;
	} 
	

	
	public String put(File f,String id,String filename,String dirname) throws IOException{
		GridFSInputFile inf=null;
		inf = gfs.createFile(f);
		inf.setFilename(dirname+'/'+filename);
		inf.setId(id);
		inf.save();
		createDir(dirname);
		return id;
	}
	
	public InputStream get(String fid){
		GridFSDBFile tmp=getGridFSFile(fid);
		if (tmp !=null) return tmp.getInputStream();
		return null;
	}
	
	public InputStream getByPath(String fname){
		GridFSDBFile tmp = gfs.findOne(fname);
		if (tmp!=null)
			return tmp.getInputStream();
		return null;
		
	}
	
	public GridFSDBFile getGridFSFile(String fid){
		BasicDBObject id =null;
		ObjectId oid=null;
		try{
		oid=new ObjectId(fid);
		}
		catch (Exception e)
		{
		 id= new BasicDBObject().append("_id", fid);
			
		}
		//GridFSDBFile f=gfs.findOne(new ObjectId(fid));
		GridFSDBFile f=null;
		if (oid!=null)
			f=gfs.findOne(oid);
		else
			f=gfs.findOne(id);
		if (f==null) return null;
		return f;
	}
	
	
	public static String getFname(String path)
	{
		return new File(path).getName();
		
	}
	
	public static String getDname(String path)
	{
		return new File(path).getParent();
	}
	
	
	public ArrayList<String> getDirFiles(String dirname){
		ArrayList<String> res=new ArrayList<String>();
		if (! dirname.endsWith("/")) dirname+="/";
		BasicDBObject query= new BasicDBObject().append("filename",java.util.regex.Pattern.compile("^"+dirname+"([^/])+$"));
		DBCursor cur=gfs.getFileList(query);
		while (cur.hasNext()){
			 DBObject f= cur.next();
			 res.add(f.get("_id").toString());
		}
		return res;	
	}
	
	public void delete(String fid)
	{
		BasicDBObject id =null;
		ObjectId oid=null;
		try{
		oid=new ObjectId(fid);
		}
		catch (Exception e)
		{
		 id= new BasicDBObject().append("_id", fid);
			
		}
		if (oid!=null)
			gfs.remove(oid);
		else
			gfs.remove(id);
	}
	
	
	public void deletebyPath(String path)
	{
		gfs.remove(path);
	}
	
	public void delDirFiles(String dirname)
	{
		for(String f : getDirFiles(dirname))
			this.delete(f);
	}
	
	public void rename(String fid,String newName)
	{
		
		BasicDBObject id =null;
		ObjectId oid=null;
		DBObject f=null;
		try{
		oid=new ObjectId(fid);
		}
		catch (Exception e)
		{
		 id= new BasicDBObject().append("_id", fid);
			
		}
		DBCollection c=db.getCollection("fs.files");
		if (oid!=null)
			f=c.findOne(oid);
		else
			f=c.findOne(id);
		newName=getDname(f.get("filename").toString())+"/"+newName;
		newName=newName.replace("\\","/");
		System.out.println(newName);
		BasicDBObject fname = new BasicDBObject().append("$set",new BasicDBObject().append("filename",newName));
		if (oid!=null)
			c.update(new BasicDBObject().append("_id", oid) ,fname,false,false,com.mongodb.WriteConcern.JOURNAL_SAFE);
		else
			c.update(id ,fname,false,false,com.mongodb.WriteConcern.JOURNAL_SAFE);
	}
	
	public Boolean existsObjectbyPath(String path)
	{
		if (gfs.findOne(path)!=null) return true;
		if (! path.endsWith("/")) path+="/";
		BasicDBObject query= new BasicDBObject().append("filename",java.util.regex.Pattern.compile("^"+path+"([^/])+$"));
		if (gfs.findOne(query)!=null) return true;
		return false;
	}
	
	public Boolean existsObjectbyUUID(String uuid)
	{
		BasicDBObject id =null;
		ObjectId oid=null;
		DBObject f=null;
		try{
		oid=new ObjectId(uuid);
		}
		catch (Exception e)
		{
		 id= new BasicDBObject().append("_id", uuid);
			
		}
		DBCollection c=db.getCollection("fs.files");
		if (oid!=null)
			f=c.findOne(oid);
		else
			f=c.findOne(id);
		if (f!=null) return true;
		return false;
	}
	
	public String getFileName(String fid)
	{
		BasicDBObject id =null;
		ObjectId oid=null;
		GridFSDBFile f=null;
		try{
		oid=new ObjectId(fid);
		}
		catch (Exception e)
		{
		 id= new BasicDBObject().append("_id", fid);
			
		}
		
		if (oid!=null)
			f=gfs.findOne(oid);
		else
			f=gfs.findOne(id);
		
		if (f!=null) return getFname(f.getFilename());
		return null;
	}

	public void close() {
		m.close();
		
	}
}
