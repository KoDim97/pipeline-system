package com.company.myExecutor;

import java.util.ArrayList;

class RLE {
    public Byte [] compress(byte [] text){
        ArrayList<Byte> newString = new ArrayList<Byte>();
        int i = 1;
        int textLength = text.length;
        int setlength;
        int startPos = 1;
        Byte nextByte;
        while(i < textLength){
            setlength = 1;
            startPos = i - 1;
            while (i < textLength && text[i] == text[i - 1]){
                setlength++;
                i++;
            }
            if (setlength != 1){
                nextByte = (byte)-setlength;
                newString.add(nextByte);
                nextByte = text[startPos];
                newString.add(nextByte);
                i++;
            }
            else {
                while (i < textLength - 1 && text[i] != text[i - 1] && text[i] != text[i + 1]){
                    setlength++;
                    i++;
                }
                if (i == textLength - 1){
                    setlength++;
                    i++;
                }
                nextByte = (byte)setlength;
                newString.add(nextByte);
                for (int j = 0; j < setlength; startPos++, j++){
                    nextByte = text[startPos];
                    newString.add(nextByte);
                }
                i++;
            }
        }
        Byte [] compressedText = null;
        compressedText = newString.toArray(new Byte[newString.size()]);
        return compressedText;
    }
    public Byte[] decompress(byte[] text){
        ArrayList<Byte> newString = new ArrayList<Byte>();
        int i = 0;
        int textLength = text.length;
        Byte nextByte;
        while (i < textLength){
            if (text[i] > 0){
                int numSymb = text[i++];
                if (numSymb > textLength - i) {
                    i--;
                    break;
                }
                for(int j = 0; j < numSymb; j++, i++){
                    nextByte = text[i];
                    newString.add(nextByte);
                }
            }
            else{
                int numSymb = -text[i++];
                if (i >= textLength) {
                    i--;
                    break;
                }
                for(int j = 0; j < numSymb; j++){
                    nextByte = text[i];
                    newString.add(nextByte);
                }
                i++;
            }
        }
        Byte [] decompressedText = null;
        decompressedText = newString.toArray(new Byte[newString.size()]);
        return decompressedText;
    }
    public byte[] toPrimitives(Byte[] oBytes) {
        byte[] bytes = new byte[oBytes.length];
        for(int i = 0; i < oBytes.length; i++) {
            bytes[i] = oBytes[i];
        }
        return bytes;
    }
    Byte[] toObjects(byte[] bytesPrim) {
        Byte[] bytes = new Byte[bytesPrim.length];
        int i = 0;
        for (byte b : bytesPrim) bytes[i++] = b; //Autoboxing
        return bytes;

    }
}