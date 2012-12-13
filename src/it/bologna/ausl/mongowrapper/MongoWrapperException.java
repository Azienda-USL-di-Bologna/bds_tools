package it.bologna.ausl.mongowrapper;

public class MongoWrapperException  extends Exception {


	private static final long serialVersionUID = 1L;

	public MongoWrapperException (String message){
		super(message);
	}

	public MongoWrapperException(String message, Throwable cause) {
		super (message,cause);
	}
}
