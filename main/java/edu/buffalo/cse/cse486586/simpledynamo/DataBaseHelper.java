package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.util.Log;

public class DataBaseHelper extends SQLiteOpenHelper {

    private static String DB_NAME = "providerDB";
    private static String  TABLE_NAME= "provider";
    public static final String COLUMN_ID = "_id";


    private SQLiteDatabase myDataBase;

    private final Context myContext;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    private static final String TABLE_CREATE = "create table "
            + TABLE_NAME + "(" + COLUMN_ID
            + " integer primary key autoincrement, " + KEY_FIELD
            + " text not null unique, " + VALUE_FIELD
            + " text not null);";
    /**
     * Constructor
     * Takes and keeps a reference of the passed context in order to access to the application assets and resources.
     * @param context
     */
    public DataBaseHelper(Context context) {

        super(context, DB_NAME, null, 1);
        this.myContext = context;
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(TABLE_CREATE);

    }

    public void flushDB() {
        SQLiteDatabase db = this.getWritableDatabase();
        db.delete(TABLE_NAME, null, null);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }

    public void addData(Uri uri, ContentValues values)
    {
        SQLiteDatabase db = this.getWritableDatabase();
        try {
            Cursor cursor = db.query(TABLE_NAME, new String[]{KEY_FIELD,VALUE_FIELD} , KEY_FIELD + "=?"
                    , new String[]{(String) values.get(KEY_FIELD)}, null, null, null);
            if(cursor.getCount()==0)
            {
                db.insert(TABLE_NAME,null,values);
            }
            else
            {
                int numberOfRowsUpdated = db.update(TABLE_NAME,values,KEY_FIELD + "=?",new String[]{(String) values.get(KEY_FIELD)});
                Log.e(TAG,"Updated Table " + numberOfRowsUpdated + " now belongs to "+values.get(KEY_FIELD)+" "+values.get(VALUE_FIELD));
            }
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }
    }
    public Cursor queryData(Uri uri, String[] projection, String selection, String[] selectionArgs,
                            String sortOrder)
    {
        SQLiteDatabase db = this.getReadableDatabase();
        Cursor cursor = null;
        if(selection.equals("@") || selection.equals("#") )
        {
            cursor= db.query(TABLE_NAME, null , null , null, null, null, null);
        }
        else
        {
            cursor = db.query(TABLE_NAME, new String[]{KEY_FIELD,VALUE_FIELD} , KEY_FIELD + "=?"
                    , new String[]{selection}, null, null, null);
        }
        return cursor;

    }

    public int removeData(String selection)
    {
        SQLiteDatabase db = this.getReadableDatabase();
        int numberOfRowsAffected = db.delete(TABLE_NAME,KEY_FIELD + "=?"
                , new String[]{selection});
        return numberOfRowsAffected;
    }
}


 