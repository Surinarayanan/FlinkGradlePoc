package org.example.bean;

/**
 * @Author : Suri Aravind @Creation Date : 22/02/24
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import java.io.Serializable;
import java.util.Map;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class AuditBean implements Serializable {

    private String context;
    private String category;
    private String eventType;
    private String actions;
    private String tenantId;
    private String productId;
    private String messageTemplateKey;
    private Map<String,Object> messageValue;
    private String eventDateTime;
    private String userId;
    private String email;
    private String userName;
    private String eventSource;
    private String clientIpAddress;
}
