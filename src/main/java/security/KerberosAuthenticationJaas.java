package security;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.PrivilegedAction;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.sun.security.auth.callback.TextCallbackHandler;

public class KerberosAuthenticationJaas {

  public static void main(String[] args) throws Exception {

    TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
      public java.security.cert.X509Certificate[] getAcceptedIssuers() {
        return null;
      }

      public void checkClientTrusted(X509Certificate[] certs, String authType) {
      }

      public void checkServerTrusted(X509Certificate[] certs, String authType) {
      }
    } };

    // Install the all-trusting trust manager
    SSLContext sc = SSLContext.getInstance("SSL");
    sc.init(null, trustAllCerts, new java.security.SecureRandom());
    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

    // Create all-trusting host name verifier
    final HostnameVerifier allHostsValid = new HostnameVerifier() {
      public boolean verify(String hostname, SSLSession session) {
        return true;
      }
    };
    LoginContext lc = null;
    try {
      lc = new LoginContext("client", new TextCallbackHandler());
      // Attempt authentication
      // You might want to do this in a "for" loop to give
      // user more than one chance to enter correct username/password
      lc.login();

      Subject.doAs(lc.getSubject(), new PrivilegedAction<Object>() {

        @Override
        public Object run() {
          try {
            // Install the all-trusting host verifier
            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);

            URL url = new URL("https://nightly55-4.gce.cloudera.com:20550/test/schema");
            HttpsURLConnection con = (HttpsURLConnection) url.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            // add request header
            // con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();
            // System.out.println("\nSending 'GET' request to URL : " + url);
            System.out.println("Response Code : " + responseCode);

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
              response.append(inputLine);
            }
            in.close();

            // print result
            System.out.println(response.toString());

            return null;
          } catch (Exception e) {
            return null;
          }
        }
      });

    } catch (LoginException le) {
      System.err.println("Authentication attempt failed" + le);
      System.exit(-1);
    }


  }

}
