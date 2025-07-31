package com.chuwa.demo.entity;


import jakarta.persistence.*;

@Entity
@Table(name = "messages")

public class MessageEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_key")
    private String messageKey;

    @Column(name = "message_value", columnDefinition = "TEXT")
    private String messageValue;

    public MessageEntity() {
    }

    public MessageEntity(Long id, String messageKey, String messageValue) {
        this.id = id;
        this.messageKey = messageKey;
        this.messageValue = messageValue;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getMessageValue() {
        return messageValue;
    }

    public void setMessageValue(String messageValue) {
        this.messageValue = messageValue;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "MessageEntity{" +
                "id=" + id +
                ", messageKey='" + messageKey + '\'' +
                ", messageValue='" + messageValue + '\'' +
                '}';
    }
}
