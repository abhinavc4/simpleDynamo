package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Scanner;
import java.util.TreeSet;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;
import com.google.gson.Gson;


public class SimpleDynamoProvider extends ContentProvider {
    boolean globalFlag = false;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	boolean standAloneMode = false;
	static String node_id = "";
	static TextView tv = null;
	static String firstAVD = "11108";
    static String secondAVD = "11116";
    static String specialPort = "specialPort";
	static String myPort = "";
	DataBaseHelper helper = null;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	static Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	static Uri mSpecialUri = buildUri("content", "edu.buffalo.cse.cse586.simpledynamo.provider");
    static Uri mVerySpecialUri = buildUri("content", "edu.buffalo.cse.cse886.simpledynamo.provider");
	private static final String IMMEDIATE_INSERT = "ImmediateInsert";
	static String successorNode = "";
	static String nextSuccessorNode = "";
	static String predecessorNode = "";
	static String greatestNode = "";
	static String smallestNode = "";

	class KeyValue {
		String key;
		String value;

		KeyValue(String inKey, String inValue) {
			key = inKey;
			value = inValue;
		}
	}

	TreeSet<String> treeOfHashedNodes = new TreeSet<String>();
	TreeSet<String> treeOfActiveNodes = new TreeSet<String>();
	String arrayOfPorts[] = {"5554", "5556", "5558", "5560", "5562"};
	HashMap<String, String> hashToPort = new HashMap<String, String>();

	private static Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	ArrayList<String> socketArrayList = new ArrayList<String>();

	Socket getSocket(int portNumber) {
		try {
			Socket tempSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					portNumber);
			return tempSocket;
		} catch (SocketTimeoutException e) {
			Log.e(TAG, "Socket Timeout Exception 1");
			e.printStackTrace();
			return null;
		} catch (StreamCorruptedException e) {
			Log.e(TAG, "Stream Corrupted Exception");
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			Log.e(TAG, "IOException2 " + portNumber);
			e.printStackTrace();
			return null;
		}
	}

	static int clientPort = 0;
	static int nextClientPort = 0;

	public void startClientServer() {
		{
			clientPort = Integer.parseInt(hashToPort.get(successorNode));
			clientPort = (clientPort * 2);
			nextClientPort = Integer.parseInt(hashToPort.get(nextSuccessorNode));
			nextClientPort = (nextClientPort * 2);
		}
	}

	public String getPredecessor(String node_id) {
		String predecessor = treeOfHashedNodes.lower(node_id);
		if (null == predecessor) {
			predecessor = treeOfHashedNodes.last();
		}
		return predecessor;
	}

	public String getSuccessor(String node_id) {
		String successor = treeOfHashedNodes.higher(node_id);
		if (null == successor) {
			successor = treeOfHashedNodes.first();
		}
		return successor;
	}

	public String getGreatest() {
		return treeOfHashedNodes.last();
	}

	public String getSmallest() {
		return treeOfHashedNodes.first();
	}


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        helper.flushDB();
        int numberOfRowsDeleted = helper.removeData(selection);
        try {
            ArrayList<String> tempSocketArrayList = new ArrayList<String>();
            String Successor = getSuccessor(node_id);
            int tempClientPort = Integer.parseInt(hashToPort.get(Successor));
            tempSocketArrayList.add(String.valueOf(tempClientPort * 2));
            Gson gson = new Gson();
            String predecessor = getPredecessor(node_id);
            tempClientPort = Integer.parseInt(hashToPort.get(predecessor));
            tempSocketArrayList.add(String.valueOf(tempClientPort * 2));
            for (String port : tempSocketArrayList) {
                MsgInterchangeFormat hMsg = new MsgInterchangeFormat(myPort, node_id);
                hMsg.mtype = MessageType.DELETE_ALL;
                String constructMessageToSend = gson.toJson(hMsg);
                //Log.e(TAG, "RETURN_QUERY_ALL Message being sent is " + constructMessageToSend + " "+port);
                Socket msgSocket = getSocket(Integer.parseInt(port));
                DataOutputStream outputControl = new DataOutputStream(msgSocket.getOutputStream());
                outputControl.writeBytes(constructMessageToSend + "\n");
            }
        }
        catch (Exception e)
        {
            Log.e(TAG,"Exception while querying");
        }
		//Log.e(TAG, "delete " + selection + " rows deleted " + numberOfRowsDeleted);
		return numberOfRowsDeleted;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		//hash the hashOfKey , if the value of the hash is
		// greater than the current node then send it to its successor
		// If the value of the hash is greater than the greatest node then send it to the root node directly

		//0 if the argument is a string lexicographically equal to this string;
		//a value less than 0 if the argument is a string lexicographically greater than this string
		//and a value greater than 0 if the argument is a string lexicographically less than this string
		String key = (String) values.get(KEY_FIELD);
		String hashOfKey = "";
		//Log.e(TAG, "Someone inserting " + key);
		try {
			hashOfKey = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			//Log.e(TAG, e.toString());
		}
		try {
			//Log.v(TAG, "insert " + key + " hash " + hashOfKey);
			if (successorNode.isEmpty() && predecessorNode.isEmpty()) {
				//Log.e(TAG,"Came here to INSERT when pred and Succ empty");
				helper.addData(uri, values);
			} else {
				//Log.e(TAG, " node_id " + node_id + " predecessorNode " + predecessorNode + "successorNode " + successorNode +
						//" hash key " + hashOfKey + "Actual key " + key + "Greatest Key " + greatestNode);
				//If first node
				boolean flag = true;
                if (0 == mVerySpecialUri.compareTo(uri)) {

                    helper.addData(uri, values);
                    return uri;
                }
                try {
                    while (globalFlag == true) {
                        Thread.sleep(100);
                    }
                }catch (InterruptedException e)
                {
                    Log.e(TAG,"Interrupted");
                }
				//Log.e(TAG, "#####" + " " + mSpecialUri + " " + uri);
				if (0 == mSpecialUri.compareTo(uri)) {

					helper.addData(uri, values);
					flag = false;
				} else if (hashOfKey.compareTo(greatestNode) > 0) {
					//If it is the greatest node
					// Insert it at the first node
					if (node_id.equals(smallestNode)) {
						MsgInterchangeFormat hMsg = new MsgInterchangeFormat(myPort, node_id);
						hMsg.mtype = MessageType.SUCCESSOR_DATA_TO_INSERT;
						hMsg.hashOfKey = hashOfKey;
						hMsg.values = values;
						Gson gson = new Gson();
						String constructMessageToSend = gson.toJson(hMsg);
						//Log.e(TAG, "Message Being sent to clientPort and next ClientPOrt is " + constructMessageToSend);
						Socket initialMessageSocket = getSocket(clientPort);
						DataOutputStream outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
						outputControl.write((constructMessageToSend + "\n").getBytes());
						initialMessageSocket = getSocket(nextClientPort);
						outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
                        outputControl.write((constructMessageToSend + "\n").getBytes());
						helper.addData(uri, values);
						flag = false;
					}
				} else if (hashOfKey.compareTo(smallestNode) < 0) {
					//If it is the greatest node
					// Insert it at the first node
					if (node_id.equals(smallestNode)) {

						MsgInterchangeFormat hMsg = new MsgInterchangeFormat(myPort, node_id);
						hMsg.mtype = MessageType.SUCCESSOR_DATA_TO_INSERT;
						hMsg.hashOfKey = hashOfKey;
						hMsg.values = values;
						Gson gson = new Gson();
						String constructMessageToSend = gson.toJson(hMsg);
						//Log.e(TAG, "Message Being sent to clientPort and next ClientPOrt is " + constructMessageToSend);
						Socket initialMessageSocket = getSocket(clientPort);
						DataOutputStream outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
						outputControl.write((constructMessageToSend + "\n").getBytes());
						initialMessageSocket = getSocket(nextClientPort);
						outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
						outputControl.write((constructMessageToSend + "\n").getBytes());
						helper.addData(uri, values);
						flag = false;
					}
				} else if ((node_id.compareTo(hashOfKey) > 0) &&
						predecessorNode.compareTo(hashOfKey) < 0) {
					MsgInterchangeFormat hMsg = new MsgInterchangeFormat(myPort, node_id);
					hMsg.mtype = MessageType.SUCCESSOR_DATA_TO_INSERT;
					hMsg.hashOfKey = hashOfKey;
					hMsg.values = values;
					Gson gson = new Gson();
					String constructMessageToSend = gson.toJson(hMsg);
					//Log.e(TAG, "Message Being sent to clientPort and next ClientPOrt is " + constructMessageToSend);
					Socket initialMessageSocket = getSocket(clientPort);
					DataOutputStream outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
					outputControl.write((constructMessageToSend + "\n").getBytes());
					initialMessageSocket = getSocket(nextClientPort);
					outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
					outputControl.write((constructMessageToSend + "\n").getBytes());
					helper.addData(uri, values);
					flag = false;
				}
				if (flag == true) {
					MsgInterchangeFormat hMsg = new MsgInterchangeFormat(myPort, node_id);
					hMsg.mtype = MessageType.DATA_TO_INSERT;
					hMsg.hashOfKey = hashOfKey;
					hMsg.values = values;
					Gson gson = new Gson();
					String appropriateNode = getSuccessor(hashOfKey);
					//Log.e(TAG, "@@@@@@@@@Succ" + getSuccessor(hashOfKey) + " hashOfKey" + hashOfKey);
					int tempClientPort = Integer.parseInt(hashToPort.get(appropriateNode));
					String constructMessageToSend = gson.toJson(hMsg);
					tempClientPort=tempClientPort*2;
					Log.e(TAG, "Message Being sent is " + constructMessageToSend +"to "+ tempClientPort);
					Socket initialMessageSocket = getSocket(tempClientPort);
                    initialMessageSocket.setSoTimeout(150);
					DataOutputStream outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
					outputControl.write((constructMessageToSend + "\n").getBytes());
                    try{
                        initialMessageSocket.setSoTimeout(100);
                        BufferedReader inputFromServer = new BufferedReader(new InputStreamReader(initialMessageSocket.getInputStream()));
                        String messageSentFromServer = inputFromServer.readLine();
                        if(messageSentFromServer == null)
                        {
                            Log.e(TAG, "Exception Finally in null");
                            hMsg.mtype = MessageType.SUCCESSOR_DATA_TO_INSERT;
                            constructMessageToSend = gson.toJson(hMsg);
                            String Successor = getSuccessor(appropriateNode);
                            int succNode = Integer.parseInt(hashToPort.get(Successor));
                            succNode = succNode*2;
                            String SuccSucc = getSuccessor(Successor);
                            int succSuccNode = Integer.parseInt(hashToPort.get(SuccSucc));
                            succSuccNode = succSuccNode*2;
                            Log.e(TAG,"Sending to "+tempClientPort+"failed , Sending instead to"+succNode+" "+succSuccNode);
                            initialMessageSocket = getSocket(succNode);
                            outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
                            outputControl.write((constructMessageToSend + "\n").getBytes());
                            initialMessageSocket = getSocket(succSuccNode);
                            outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
                            outputControl.write((constructMessageToSend + "\n").getBytes());
                        }
                        else
                        {
                            Log.e(TAG,"messageSentFromServer is Null"+messageSentFromServer);
                        }
                    }
                    catch (Exception e)
                    {
                        Log.e(TAG, "Exception Finally " + e.toString());
                        hMsg.mtype = MessageType.SUCCESSOR_DATA_TO_INSERT;
                        constructMessageToSend = gson.toJson(hMsg);
                        String Successor = getSuccessor(appropriateNode);
                        int succNode = Integer.parseInt(hashToPort.get(Successor));
                        succNode = succNode*2;
                        String SuccSucc = getSuccessor(Successor);
                        int succSuccNode = Integer.parseInt(hashToPort.get(SuccSucc));
                        succSuccNode = succSuccNode*2;
                        Log.e(TAG,"Sending to "+tempClientPort+"failed , Sending instead to"+succNode+" "+succSuccNode);
                        initialMessageSocket = getSocket(succNode);
                        outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
                        outputControl.write((constructMessageToSend + "\n").getBytes());
                        initialMessageSocket = getSocket(succSuccNode);
                        outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
                        outputControl.write((constructMessageToSend + "\n").getBytes());
                        e.printStackTrace();
                    }
				}
			}
		} catch (IOException e) {
			Log.e(TAG, "#########Insert ###################"+e.toString());
            e.printStackTrace();
		}
		return uri;
	}

	@Override
	public boolean onCreate() {

		helper = new DataBaseHelper(getContext());
        helper.flushDB();
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		try {
			for (String port : arrayOfPorts) {
				hashToPort.put(genHash(port), port);
				treeOfHashedNodes.add(genHash(port));
				if(false == port.equals("5554"))
				{
					socketArrayList.add(String.valueOf((Integer.parseInt(port) * 2)));
				}
			}
			node_id = genHash(portStr);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Runnable r  = new QueryThread();
            new Thread(r).start();


		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "NoSuchAlgorithmException Exception Thrown " + e.toString());
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		} catch (Exception e)
		{
			Log.e(TAG, "OnCreate ServerSocket");
			return false;
		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		if (selection.equals("*")) {
            try {
                while (globalFlag == true) {
                    Thread.sleep(100);
                }
            }catch (InterruptedException e)
            {
                Log.e(TAG,"Interrupted");
            }
			if (successorNode.isEmpty() && predecessorNode.isEmpty()) {
				Cursor cursor = helper.queryData(mUri, projection, "#", selectionArgs, sortOrder);
				return cursor;
			} else {
				try {
					MsgInterchangeFormat hMsg = new MsgInterchangeFormat(myPort, node_id);
					hMsg.mtype = MessageType.QUERY_ALL;
					hMsg.selection = "*";
					hMsg.portNumber = myPort;
					hMsg.predecessor = predecessorNode;
					Gson gson = new Gson();
					String constructMessageToSend = gson.toJson(hMsg);
					//Log.e(TAG, "Message Being sent is " + constructMessageToSend);
					Socket initialMessageSocket = getSocket(Integer.parseInt(firstAVD));
					DataOutputStream outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
					outputControl.writeBytes(constructMessageToSend + "\n");
                    initialMessageSocket.setSoTimeout(3000);
                    BufferedReader inputFromServer = null;
                    String messageSentFromServer = "";
                    Log.e(TAG,"* has come Begin");
                    try {
                        inputFromServer = new BufferedReader(new InputStreamReader(initialMessageSocket.getInputStream()));
                        messageSentFromServer = inputFromServer.readLine();
                        if(messageSentFromServer == null)
                        {
                            Log.e(TAG,"TImed out");
                            initialMessageSocket = getSocket(Integer.parseInt(secondAVD));

                            outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
                            outputControl.writeBytes(constructMessageToSend + "\n");
                            initialMessageSocket.setSoTimeout(3000);
                            inputFromServer = new BufferedReader(new InputStreamReader(initialMessageSocket.getInputStream()));
                            messageSentFromServer = inputFromServer.readLine();
                        }
                    }
                    catch (SocketTimeoutException e)
                    {
                        Log.e(TAG,"Timed out in Exception");
                        initialMessageSocket = getSocket(Integer.parseInt(secondAVD));
                        outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
                        initialMessageSocket.setSoTimeout(3000);
                        outputControl.writeBytes(constructMessageToSend + "\n");
                        inputFromServer = new BufferedReader(new InputStreamReader(initialMessageSocket.getInputStream()));
                        messageSentFromServer = inputFromServer.readLine();
                    }
					MsgInterchangeFormat msg = gson.fromJson(messageSentFromServer, MsgInterchangeFormat.class);
					//Log.e(TAG, "Value received at original server is for Query All is " + messageSentFromServer);
					//http://stackoverflow.com/questions/9917935/adding-rows-into-cursor-manually
					MergeCursor mergeCursor = null;
					Cursor cursor = null;
					MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
					for (KeyValue kv : msg.alist) {
						if (kv.key.equals("#")) {
							continue;
						}
						matrixCursor.addRow(new Object[]{kv.key, kv.value});
					}
					mergeCursor = new MergeCursor(new Cursor[]{matrixCursor, cursor});
					return mergeCursor;
				} catch (IOException e) {
					//Log.e(TAG, e.toString());
				}
			}
		}
        Cursor cursor = null;
        if(uri.equals(mSpecialUri))
        {
            cursor = helper.queryData(mUri, projection, selection, selectionArgs, sortOrder);
            return cursor;
        }
        uri = mUri;
        try {
            while (globalFlag == true) {
                Thread.sleep(100);
            }
        }catch (InterruptedException e)
        {
            Log.e(TAG,"Interrupted");
        }
		cursor= helper.queryData(uri, projection, selection, selectionArgs, sortOrder);
		//Log.e(TAG, "query " + selection);
		if (selection.equals("@")) {
			return cursor;
		}
		if (selection.equals("#") &&
				((cursor != null) &&
    					(cursor.getCount() == 0))) {
             return cursor;
		}
		if ((cursor != null) && (cursor.getCount() > 0)) {
			//Log.e(TAG, "Query Successful" + cursor.toString());
			return cursor;
		} else {
			try {
				MsgInterchangeFormat hMsg = new MsgInterchangeFormat(myPort, node_id);
				hMsg.mtype = MessageType.QUERY;
				hMsg.selection = selection;
				hMsg.portNumber = myPort;
				Gson gson = new Gson();
                String hashOfKey = genHash(selection);
				String constructMessageToSend = gson.toJson(hMsg);
                String appropriateNode = getSuccessor(hashOfKey);
                int tempClientPort = Integer.parseInt(hashToPort.get(appropriateNode));
                tempClientPort=tempClientPort*2;
                Log.e(TAG,"Query " + tempClientPort + " " + selection);
				//Log.e(TAG, "Message Being sent is " + constructMessageToSend);
				Socket initialMessageSocket = getSocket(tempClientPort);
				DataOutputStream outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
				outputControl.writeBytes(constructMessageToSend + "\n");
                initialMessageSocket.setSoTimeout(500);
                BufferedReader inputFromServer = null;
                String messageSentFromServer = "";
                try {
                    inputFromServer = new BufferedReader(new InputStreamReader(initialMessageSocket.getInputStream()));
                    messageSentFromServer = inputFromServer.readLine();
                    if(messageSentFromServer == null)
                    {
                        String Successor = getSuccessor(appropriateNode);
                        int succTempClientPort = Integer.parseInt(hashToPort.get(Successor));
                        succTempClientPort = succTempClientPort * 2;
                        initialMessageSocket = getSocket(succTempClientPort);
                        initialMessageSocket.setSoTimeout(500);
                        Log.e(TAG, "Sending message to " + tempClientPort + "failed . Sending message to " + succTempClientPort + " " + selection);
                        outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
                        outputControl.writeBytes(constructMessageToSend + "\n");
                        inputFromServer = new BufferedReader(new InputStreamReader(initialMessageSocket.getInputStream()));
                        messageSentFromServer = inputFromServer.readLine();
                        if(messageSentFromServer == null)
                        {
                            String newSuccessor = getSuccessor(Successor);
                            int succSuccTempClientPort = Integer.parseInt(hashToPort.get(newSuccessor ));
                            succSuccTempClientPort = succSuccTempClientPort * 2;
                            initialMessageSocket = getSocket(succSuccTempClientPort);
                            initialMessageSocket.setSoTimeout(500);
                            Log.e(TAG, "Sending message to " + succTempClientPort + "failed . Sending message to " + succSuccTempClientPort + " " + selection);
                            outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
                            outputControl.writeBytes(constructMessageToSend + "\n");
                            inputFromServer = new BufferedReader(new InputStreamReader(initialMessageSocket.getInputStream()));
                            messageSentFromServer = inputFromServer.readLine();
                        }
                    }
                }
                catch (SocketTimeoutException e) {
                        Log.e(TAG,"Caught a Socket Timeout exception");
                        String Successor = getSuccessor(appropriateNode);
                        int succTempClientPort = Integer.parseInt(hashToPort.get(Successor));
                        succTempClientPort = succTempClientPort * 2;
                        initialMessageSocket = getSocket(succTempClientPort);
                        initialMessageSocket.setSoTimeout(500);
                        Log.e(TAG, "Sending message to " + tempClientPort + "failed . Sending message to " + succTempClientPort + " " + selection);
                        outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
                        outputControl.writeBytes(constructMessageToSend + "\n");
                        initialMessageSocket.setSoTimeout(500);
                        inputFromServer = new BufferedReader(new InputStreamReader(initialMessageSocket.getInputStream()));
                        messageSentFromServer = inputFromServer.readLine();
                    if(messageSentFromServer == null)
                    {
                        String newSuccessor = getSuccessor(Successor);
                        int succSuccTempClientPort = Integer.parseInt(hashToPort.get(newSuccessor ));
                        succSuccTempClientPort = succSuccTempClientPort * 2;
                        initialMessageSocket = getSocket(succSuccTempClientPort);
                        initialMessageSocket.setSoTimeout(500);
                        Log.e(TAG, "Sending message to " + succTempClientPort + "failed . Sending message to " + succSuccTempClientPort + " " + selection);
                        outputControl = new DataOutputStream(initialMessageSocket.getOutputStream());
                        outputControl.writeBytes(constructMessageToSend + "\n");
                        inputFromServer = new BufferedReader(new InputStreamReader(initialMessageSocket.getInputStream()));
                        messageSentFromServer = inputFromServer.readLine();
                    }

                    }
				//Log.e(TAG, "Value received at original server is " + messageSentFromServer);
				//http://stackoverflow.com/questions/9917935/adding-rows-into-cursor-manually

				MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
				matrixCursor.addRow(new Object[]{selection, messageSentFromServer});
				MergeCursor mergeCursor = new MergeCursor(new Cursor[]{matrixCursor, cursor});
				return mergeCursor;
			}catch (NoSuchAlgorithmException e)
            {
                Log.e(TAG,e.toString());
            }
            catch (IOException e) {
				Log.e(TAG, e.toString());
			}
		}
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	public enum MessageType {
		HELLO,
		SENDING_SUCC_PRED,
		SENDING_SUCC_PRED_UPDATE,
		QUERY,
		QUERY_ALL,
		DATA_TO_INSERT,
		SUCCESSOR_DATA_TO_INSERT,
		RETURN_QUERY_ALL,
        DELETE_ALL,
		INVALID
	}

	private class MsgInterchangeFormat {
		String hashOfKey;
		String portNumber;
		String hashedPortNumber;
		String successor;
		String predecessor;
		String greatest;
		String smallest;
		ContentValues values;
		MessageType mtype;
		String selection;
		ArrayList<KeyValue> alist = new ArrayList<KeyValue>();

		MsgInterchangeFormat() {
			hashOfKey = "";
			portNumber = "";
			hashedPortNumber = "";
			successor = "";
			predecessor = "";
			mtype = MessageType.INVALID;
		}

		MsgInterchangeFormat(String inPortNumber, String inHashedPortNumber) {
			this();
			portNumber = inPortNumber;
			hashedPortNumber = inHashedPortNumber;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {
				successorNode = getSuccessor(node_id);
				nextSuccessorNode = getSuccessor(successorNode);
				predecessorNode = getPredecessor(node_id);
				greatestNode = getGreatest();
				smallestNode = getSmallest();
				//Log.e(TAG, " Values are NodeID " + node_id + " successorNode " + successorNode +
						//" nextSuccessorNode " + nextSuccessorNode + " predecessorNode " + predecessorNode + "greatestNode " + greatestNode
				//+"smallestNode "+smallestNode);
			startClientServer();
			return null;
		}
    	protected void onProgressUpdate(Void... strings) {
			return;
		}

	}



	private class ServerTask extends AsyncTask<ServerSocket, MsgInterchangeFormat, Void> {
		@Override
        /*
        Override this method to perform a computation on a background thread.
        The specified parameters are the parameters passed to execute(Params...) by the caller of this task
        This method can call publishProgress(Progress...) to publish updates on the UI thread.
         */
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Scanner sc = null;

			try {
				//while (true) {
				Log.e(TAG, "Server Thread Invoked " + myPort);
				Socket listenSocket = null;
				while (true) {
					listenSocket = serverSocket.accept();
					BufferedReader inputFromClient = new BufferedReader(new InputStreamReader(listenSocket.getInputStream()));
					String messageSentFromClient = inputFromClient.readLine();
					Gson gson = new Gson();
					MsgInterchangeFormat msg = gson.fromJson(messageSentFromClient, MsgInterchangeFormat.class);
					Log.e(TAG, msg.mtype.toString() + "Client Message received From " + msg.portNumber + " and " + msg.hashedPortNumber);
					switch (msg.mtype) {
						case HELLO:
							treeOfHashedNodes.add(msg.hashedPortNumber);
							treeOfActiveNodes.add(msg.portNumber);
							predecessorNode = getPredecessor(node_id);
							successorNode = getSuccessor(node_id);
							greatestNode = getGreatest();
							smallestNode = getSmallest();
							startClientServer();
							//Log.e(TAG, "Current Contents of Priority Queue Are " + treeOfHashedNodes.size());
							for (String treeOfHashedNode : treeOfHashedNodes) {
								//Log.e(TAG, treeOfHashedNode);
							}
							//Send a reply to everybody
							MsgInterchangeFormat hMsgReply = new MsgInterchangeFormat();
							hMsgReply.predecessor = getPredecessor(msg.hashedPortNumber);
							hMsgReply.successor = getSuccessor(msg.hashedPortNumber);
							hMsgReply.greatest = getGreatest();
							hMsgReply.smallest = getSmallest();
							smallestNode = getSmallest();
							String constructMessageToSend = gson.toJson(hMsgReply);
							//Log.e(TAG, "Message being sent is " + constructMessageToSend);
							DataOutputStream outputControl = new DataOutputStream(listenSocket.getOutputStream());
							outputControl.writeBytes(constructMessageToSend + "\n");

							for (String port : socketArrayList) {
								MsgInterchangeFormat hMsg = new MsgInterchangeFormat(myPort, node_id);
								hMsg.mtype = MessageType.SENDING_SUCC_PRED_UPDATE;
								int portNumber = (Integer.parseInt(port)) / 2;
								String hashedPortNumber = genHash(String.valueOf(portNumber));
								hMsg.hashedPortNumber = hashedPortNumber;
								hMsg.predecessor = getPredecessor(hashedPortNumber);
								hMsg.successor = getSuccessor(hashedPortNumber);
								hMsg.greatest = getGreatest();
								hMsg.smallest = getSmallest();
								constructMessageToSend = gson.toJson(hMsg);
								//Log.e(TAG, "UPDATED Message being sent is " + constructMessageToSend);
								Socket msgSocket = getSocket(Integer.parseInt(port));
								outputControl = new DataOutputStream(msgSocket.getOutputStream());
								outputControl.writeBytes(constructMessageToSend + "\n");
							}
							socketArrayList.add(msg.portNumber);
							break;
						case SENDING_SUCC_PRED_UPDATE:
							//Log.e(TAG, msg.mtype.toString() + " Updated Successor and Predecessor " + msg.successor + msg.predecessor);
							predecessorNode = msg.predecessor;
							successorNode = msg.successor;
							nextSuccessorNode = getSuccessor(successorNode);
							greatestNode = msg.greatest;
							smallestNode = msg.smallest;
							startClientServer();
							break;
						case DATA_TO_INSERT:
							//Log.e(TAG, msg.mtype.toString() + " Received data to insert " + (String) msg.values.get(KEY_FIELD));
							insert(mUri, msg.values);
                            outputControl = new DataOutputStream(listenSocket.getOutputStream());
                            outputControl.writeBytes("Hello" + "\n");
							break;
						case SUCCESSOR_DATA_TO_INSERT:
							//Log.e(TAG, msg.mtype.toString() + " Received Special data to insert " + (String) msg.values.get(KEY_FIELD));
							insert(mSpecialUri, msg.values);

							break;
						case QUERY:
							//Log.e(TAG, msg.mtype.toString() + "Received Query" + msg);
							Cursor resultCursor = query(mUri, null,
									msg.selection, null, null);
							int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
							resultCursor.moveToFirst();
							String returnValue = resultCursor.getString(valueIndex);
							//Log.e(TAG, "Message being sent is " + returnValue);
							outputControl = new DataOutputStream(listenSocket.getOutputStream());
							outputControl.writeBytes(returnValue + "\n");
							break;
                        case DELETE_ALL:
                            helper.flushDB();
                            break;
                        case RETURN_QUERY_ALL:
							MsgInterchangeFormat tempReply = new MsgInterchangeFormat();
                            if (msg.portNumber.equals(specialPort))
                            {
                                resultCursor = query(mSpecialUri, null,
                                        "#", null, null);
                            }
                            else
                            {
                                resultCursor = query(mUri, null,
                                        "#", null, null);
                            }
							if (resultCursor.moveToFirst() ){
								String[] columnNames = resultCursor.getColumnNames();
								do {
									String key = "";
									for (String name: columnNames) {
										if(name.equals("_id"))
										{
											continue;
										}
										else if(name.equals(KEY_FIELD))
										{
											key = resultCursor.getString(resultCursor.getColumnIndex(name));
										}
										else
										{
											KeyValue kv = new KeyValue(key,resultCursor.getString(resultCursor.getColumnIndex(name)));
											tempReply.alist.add(kv);
										}
									}
								} while (resultCursor.moveToNext());
							}
							constructMessageToSend = gson.toJson(tempReply);
							//Log.e(TAG, "Message being RETURN_QUERY_ALL is " + constructMessageToSend);
							outputControl = new DataOutputStream(listenSocket.getOutputStream());
							outputControl.writeBytes(constructMessageToSend + "\n");
							break;
						case QUERY_ALL:
							MsgInterchangeFormat finalReply = new MsgInterchangeFormat();
							//Log.e(TAG, "Current Contents of Socket Array List Are " + socketArrayList.size());
							for (String treeOfHashedNode : socketArrayList) {
								//Log.e(TAG, treeOfHashedNode);
							}
							for (String port : socketArrayList) {
                                MsgInterchangeFormat hMsg = new MsgInterchangeFormat(myPort, node_id);
                                hMsg.mtype = MessageType.RETURN_QUERY_ALL;
                                constructMessageToSend = gson.toJson(hMsg);
                                //Log.e(TAG, "RETURN_QUERY_ALL Message being sent is " + constructMessageToSend + " "+port);
                                Socket msgSocket = getSocket(Integer.parseInt(port));
                                outputControl = new DataOutputStream(msgSocket.getOutputStream());
                                outputControl.writeBytes(constructMessageToSend + "\n");
                                BufferedReader inputFromServer = null;
                                msgSocket.setSoTimeout(1000);
                                Log.e(TAG, "Asking for data from " + port);
                                inputFromServer = new BufferedReader(new InputStreamReader(msgSocket.getInputStream()));
                                String tempStr = "";
                                try {
                                    tempStr = inputFromServer.readLine();
                                    if (tempStr == null) {
                                        Log.e(TAG, port + "Is DOWN");
                                        continue;
                                    }
                                }
                                catch(SocketTimeoutException e)
                                {
                                    Log.e(TAG, port + "Is DOWN");
                                    continue;
                                }
                                MsgInterchangeFormat tempMsg = gson.fromJson(tempStr, MsgInterchangeFormat.class);
								finalReply.alist.addAll(tempMsg.alist);
								//messageSentFromServer += inputFromServer.readLine();
							}
							//Add your own messages also

							resultCursor = query(mUri, null,
									"#", null, null);
							if (resultCursor.moveToFirst() ){
								String[] columnNames = resultCursor.getColumnNames();
								do {
									String key = "";
									for (String name: columnNames) {
										if(name.equals("_id"))
										{
											continue;
										}
										else if(name.equals(KEY_FIELD))
										{
											key = resultCursor.getString(resultCursor.getColumnIndex(name));
										}
										else
										{
											KeyValue kv = new KeyValue(key,resultCursor.getString(resultCursor.getColumnIndex(name)));
											finalReply.alist.add(kv);
										}
									}
								} while (resultCursor.moveToNext());
							}
							constructMessageToSend = gson.toJson(finalReply);
							//Log.e(TAG, "QUERY_ALL Message being sent is " + constructMessageToSend);
							outputControl = new DataOutputStream(listenSocket.getOutputStream());
							outputControl.writeBytes(constructMessageToSend + "\n");
							break;
					}
                        /*for (sockNPort currSocket : socketArrayList) {
                            HelloMessageReply hMsgReply = new HelloMessageReply();
                            Log.e(TAG, String.valueOf(currSocket.port));
                            int portNumber = (Integer.parseInt(currSocket.port)) / 2;
                            String hashedPortNumber = genHash(String.valueOf(portNumber));
                            hMsgReply.predecessor = getPredecessor(hashedPortNumber);
                            hMsgReply.successor = getSuccessor(hashedPortNumber);
                            String constructMessageToSend = gson.toJson(hMsgReply);
                            Log.e(TAG, "Message being sent is " + constructMessageToSend);
                            DataOutputStream outputControl = new DataOutputStream(currSocket.socket.getOutputStream());
                            outputControl.writeBytes(constructMessageToSend + "\n");
                        }*/
				}
			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG, "No such Algorithm Exception");
				e.printStackTrace();
			} catch (SocketTimeoutException e) {
				Log.e(TAG, "Socket Timeout Exception 4");
				e.printStackTrace();
			} catch (StreamCorruptedException e) {
				Log.e(TAG, "Stream Corrupted Exception");
				e.printStackTrace();
			} catch (IOException e) {
				Log.e(TAG, "IO Exception 6");
				e.printStackTrace();
			}
			catch (Exception e) {
				Log.e(TAG, "Server Exception"+ e.toString());
				e.printStackTrace();
			}
			return null;
		}

		/*
        Runs on the UI thread after publishProgress(Progress...) is invoked.
        The specified values are the values passed to publishProgress(Progress...).
        */
		protected void onProgressUpdate(MsgInterchangeFormat... strings) {
			Gson gson = new Gson();
			String constructMessageToSend = gson.toJson(strings[0]);
			String strReceived = "Successor " + strings[0].successor +
					"Predecessor " + strings[0].predecessor;
			tv.append("\t\t\t\t" + strReceived + "\t\n");
			return;
		}

	}
    public class QueryThread implements Runnable{
        public void run()
        {
            try
            {
                globalFlag = true;
                MsgInterchangeFormat finalReply = new MsgInterchangeFormat();
                ArrayList<String> tempSocketArrayList = new ArrayList<String>();
                String Successor = getSuccessor(node_id);
                int tempClientPort = Integer.parseInt(hashToPort.get(Successor));
                tempSocketArrayList.add(String.valueOf(tempClientPort*2));
                Gson gson = new Gson();
                String predecessor = getPredecessor(node_id);
                tempClientPort = Integer.parseInt(hashToPort.get(predecessor));
                tempSocketArrayList.add(String.valueOf(tempClientPort * 2));
                for (String port : tempSocketArrayList) {
                    MsgInterchangeFormat hMsg = new MsgInterchangeFormat(myPort, node_id);
                    hMsg.mtype = MessageType.RETURN_QUERY_ALL;
                    hMsg.portNumber = specialPort;
                    String constructMessageToSend = gson.toJson(hMsg);
                    //Log.e(TAG, "RETURN_QUERY_ALL Message being sent is " + constructMessageToSend + " "+port);
                    Socket msgSocket = getSocket(Integer.parseInt(port));
                    DataOutputStream outputControl = new DataOutputStream(msgSocket.getOutputStream());
                    outputControl.writeBytes(constructMessageToSend + "\n");
                    BufferedReader inputFromServer = null;
                    inputFromServer = new BufferedReader(new InputStreamReader(msgSocket.getInputStream()));
                    String tempStr = inputFromServer.readLine();
                    //Log.e(TAG,"TempSTR is " +tempStr);
                    if(null!=tempStr)
                    {
                        MsgInterchangeFormat tempMsg = gson.fromJson(tempStr, MsgInterchangeFormat.class);
                        if(tempMsg.alist.size()>0)
                        {
                            finalReply.alist.addAll(tempMsg.alist);
                        }
                    }

                    //messageSentFromServer += inputFromServer.readLine();
                }

                ContentValues[] cv = new ContentValues[finalReply.alist.size()];
                for (int i = 0; i < finalReply.alist.size(); i++) {
                    cv[i] = new ContentValues();
                    KeyValue kv = finalReply.alist.get(i);
                    if (kv.key.equals("#")) {
                        continue;
                    }
                    cv[i].put(KEY_FIELD, kv.key);
                    boolean insertFlag = false;
                    String nodeTOCompare = getSuccessor(genHash(kv.key));
                    String firstBackup = getSuccessor(nodeTOCompare);
                    String secondBackup = getSuccessor(firstBackup);
                    if(node_id.equals(nodeTOCompare))
                    {
                        insertFlag = true;
                    }
                    else if(node_id.equals(firstBackup))
                    {
                        insertFlag = true;
                    }
                    else if(node_id.equals(secondBackup))
                    {
                        insertFlag = true;
                    }
                    else
                    {
                        //Log.e(TAG,kv.key + "### Does not belong");
                        //Log.e(TAG,kv.key+" nodeID " + node_id + " firstBackup " + firstBackup + " secondBackup " + secondBackup + " nodeTOCompare " + nodeTOCompare);
                    }

                    cv[i].put(VALUE_FIELD, kv.value);
                    if(true == insertFlag)
                    {
                        insert(mVerySpecialUri,cv[i]);
                    }
                }
                globalFlag = false;
            }
            catch (Exception e)
            {
                globalFlag = false;
                Log.e(TAG,e.toString());
                e.printStackTrace();
            }


        }
    }
}
