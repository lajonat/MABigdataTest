package com.test.Processor;

public class ProcessException extends Exception {
    ProcessException(Throwable e) {super(e);}
    ProcessException(String message) {super(message);}
    ProcessException(String message, Throwable e) {super(message, e);}
}
