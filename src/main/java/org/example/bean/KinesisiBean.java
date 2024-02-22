package org.example.bean;

/**
 * @Author : Suri Aravind @Creation Date : 22/02/24
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import java.io.Serializable;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class KinesisiBean implements Serializable {
private String key1;
private Integer integerKey;
private boolean booleanKey;
private String anotherString;
}
