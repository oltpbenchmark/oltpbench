package com.oltpbenchmark.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.util.json.JSONException;
import com.oltpbenchmark.util.json.JSONObject;
import com.oltpbenchmark.util.json.JSONStringer;

public abstract class ResultObject implements JSONSerializable {

    public static class DBCollection extends ResultObject {
        private final List<JSONSerializable> items;
        
        public DBCollection() {
            items = new ArrayList<JSONSerializable>();
        }
        
        public DBCollection add(JSONSerializable item) {
            items.add(item);
            return this;
        }

        @Override
        public String toJSONString() {
            return (JSONUtil.toJSONString(this));
        }

        @Override
        public void toJSON(JSONStringer stringer) throws JSONException {
            for (JSONSerializable item : items) {
                item.toJSON(stringer);
            }
            
        }
    }
    
    public static class DBEntry extends ResultObject {
        public String key;
        public Object value;
        
        public DBEntry(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toJSONString() {
            return (JSONUtil.toJSONString(this));
        }

        @Override
        public void toJSON(JSONStringer stringer) throws JSONException {
            stringer.key(key);
            if (value instanceof JSONSerializable) {
                stringer.object();
                ((JSONSerializable) value).toJSON(stringer);
                stringer.endObject();
            } else {
                JSONUtil.writeFieldValue(stringer, DBEntry.class, value);
            }
            
        }
        
    }

    @Override
    public void save(String output_path) throws IOException {
        throw new UnsupportedOperationException();
        
    }

    @Override
    public void load(String input_path) throws IOException {
        throw new UnsupportedOperationException();
        
    }
    
    @Override
    public void fromJSON(JSONObject json_object) throws JSONException {
        throw new UnsupportedOperationException();
        
    }
    
}
