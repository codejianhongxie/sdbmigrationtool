package com.codejianhongxie.util;

import com.codejianhongxie.util.sm2.DigitalUtil;
import com.codejianhongxie.util.sm2.SM2Bean;
import com.codejianhongxie.util.sm2.SM2Util;
import org.bouncycastle.crypto.AsymmetricCipherKeyPair;
import org.bouncycastle.crypto.params.ECPrivateKeyParameters;
import org.bouncycastle.crypto.params.ECPublicKeyParameters;
import org.bouncycastle.math.ec.ECPoint;

import java.math.BigInteger;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/26 17:27
 */
public class PasswordUtil {

    private final static String SM2 = "sm2";
    private final static String PUBLIC_KEY = "046F1CA51CDC3AA3C258DA64A53BFA6EC7F14AE09328C0129CA091966738E5C7D148C3B909A958806CA636B25E23ECBE12C0F4CE5CC9881FB02EF6B7C7077AF10B";
    private final static String PRIVATE_KEY = "7DE7703E7353DC4E7E098ADCD2BDA0196C0B05242BC84D4C90C2DB061AB99AFE";

    public static String passwordEncrypt(String password) throws IllegalArgumentException {
        return passwordEncrypt(SM2, PUBLIC_KEY, password);
    }

    public static String passwordEncrypt(String decryptType, String publicKey, String password) throws IllegalArgumentException {

        if (SM2.equalsIgnoreCase(decryptType)) {
            password = SM2Util.encrypt(DigitalUtil.hexToByte(publicKey), password.getBytes());
        } else {
            System.out.println("暂未实现" + decryptType + "类型的密码加密！");
        }
        return password;
    }

    public static String passwordDecrypt(String password) throws IllegalArgumentException {
        return passwordDecrypt(SM2, PRIVATE_KEY, password);
    }

    public static String passwordDecrypt(String decryptType, String privateKey, String password) throws IllegalArgumentException {

        if (SM2.equalsIgnoreCase(decryptType)) {
            password = new String(SM2Util.decrypt(DigitalUtil.hexToByte(privateKey), DigitalUtil.hexToByte(password)));
        } else {
            System.out.println("暂未实现" + decryptType + "类型的密码解密！");
        }
        return password;
    }

    public static void generateEncryptPassword(String password) {
        SM2Bean sm2 = SM2Bean.Instance();
        AsymmetricCipherKeyPair key = sm2.ecc_key_pair_generator.generateKeyPair();
        ECPrivateKeyParameters ecpriv = (ECPrivateKeyParameters) key.getPrivate();
        ECPublicKeyParameters ecpub = (ECPublicKeyParameters) key.getPublic();
        BigInteger privateKey = ecpriv.getD();
        ECPoint publicKey = ecpub.getQ();

        String passwordPublicKey = DigitalUtil.byteToHex(publicKey.getEncoded(false));
        String passwordPrivateKey = DigitalUtil.byteToHex(privateKey.toByteArray());
        String encryptPassword = SM2Util.encrypt(DigitalUtil.hexToByte(passwordPublicKey), password.getBytes());

        System.out.println("公钥: " + passwordPublicKey);
        System.out.println("私钥: " + passwordPrivateKey);
        System.out.println("加密: " + encryptPassword);
    }

    public static void main(String[] args) {

        if (args.length <= 0) {
            throw new IllegalArgumentException("请输入待加密的密码");
        }
        System.out.println(passwordEncrypt(SM2, PUBLIC_KEY, args[0]));
    }
}
