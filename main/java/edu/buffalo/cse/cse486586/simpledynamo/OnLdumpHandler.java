package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class OnLdumpHandler implements OnClickListener {

    private static final String TAG = OnLdumpHandler.class.getName();
    private static final int TEST_CNT = 50;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;
    //private final ContentValues[] mContentValues;
    static String tableString = "";
    public OnLdumpHandler(TextView _tv, ContentResolver _cr) {
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        //mContentValues = initTestValues();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public void onClick(View v) {
        new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    private class Task extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... params) {
            if (testQuery()) {
                publishProgress("Query success\n" + tableString);
            } else {
                publishProgress("Query fail\n");
            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            mTextView.append(strings[0]);

            return;
        }

        private boolean testQuery() {
            try {
                String key = "@";
                Cursor resultCursor = mContentResolver.query(mUri, null,
                        key, null, null);
                if (resultCursor == null) {
                    Log.e(TAG, "Result null");
                    throw new Exception();
                }

                int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                if (keyIndex == -1 || valueIndex == -1) {
                    Log.e(TAG, "Wrong columns");
                    resultCursor.close();
                    throw new Exception();
                }

                resultCursor.moveToFirst();

                String returnKey = resultCursor.getString(keyIndex);
                String returnValue = resultCursor.getString(valueIndex);

                //http://stackoverflow.com/questions/27003486/printing-all-rows-of-a-sqlite-database-in-android

                if (resultCursor.moveToFirst() ){
                    String[] columnNames = resultCursor.getColumnNames();
                    do {
                        for (String name: columnNames) {
                            tableString += String.format("%s: %s\n", name,
                                    resultCursor.getString(resultCursor.getColumnIndex(name)));
                        }
                        tableString += "\n";

                    } while (resultCursor.moveToNext());
                }
                resultCursor.close();
            }
            catch (Exception e) {
                return false;
            }

            return true;
        }
    }
}
