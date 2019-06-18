package com.redhat.events;

import java.util.Objects;

public class AlertMapObj {
    private String userId;
    private String noOfAttempts;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getNoOfAttempts() {
        return noOfAttempts;
    }

    public void setNoOfAttempts(String noOfAttempts) {
        this.noOfAttempts = noOfAttempts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AlertMapObj)) return false;
        AlertMapObj that = (AlertMapObj) o;
        return Objects.equals(getUserId(), that.getUserId());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getUserId());
    }

    @Override
    public String toString() {
        return "AlertMapObj{" +
                "userId='" + userId + '\'' +
                ", noOfAttempts='" + noOfAttempts + '\'' +
                '}';
    }
}
