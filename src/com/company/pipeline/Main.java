package com.company.pipeline;

public class Main {
    public static void main(String[] args) {
        if (args.length != 1){
            System.out.println("Error! Check logs.txt for more details");
            return;
        }
        Manager manager = new Manager(args[0]);
        if (manager.getErrors() == Manager.ERRORS.NO_ERRORS){
            manager.Run();
        }
    }
}
