package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

/**
 * @Author : Suri Aravind
 * @Creation Date : 21/02/24
 */
public class JSONValueDeserializationSchema implements org.apache.flink.api.common.serialization.DeserializationSchema<AuditBean> {

    final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public AuditBean deserialize(byte[] bytes) throws IOException {
        try{
            System.out.println(new String(bytes));
            return objectMapper.readValue(bytes, AuditBean.class);
        }catch (Exception exception){
            return AuditBean.builder().build();
        }
    }

    @Override
    public boolean isEndOfStream(AuditBean stringObjectMap) {
        return false;
    }

    @Override
    public TypeInformation<AuditBean> getProducedType() {
        return TypeInformation.of(AuditBean.class);
    }
}
