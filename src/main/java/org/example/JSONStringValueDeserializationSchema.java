package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.dynamodbv2.xspec.S;

/**
 * @Author : Suri Aravind
 * @Creation Date : 21/02/24
 */
public class JSONStringValueDeserializationSchema implements DeserializationSchema<KinesisiBean> {

    final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public KinesisiBean deserialize(byte[] bytes) throws IOException {
        try{
            System.out.println("Data :"+ new String(bytes));
            return objectMapper.readValue(bytes, KinesisiBean.class);
        }catch (Exception exception){
            return KinesisiBean.builder().build();
        }
    }

    @Override
    public boolean isEndOfStream(KinesisiBean stringObjectMap) {
        return false;
    }

    @Override
    public TypeInformation<KinesisiBean> getProducedType() {
        return TypeInformation.of(KinesisiBean.class);
    }
}
