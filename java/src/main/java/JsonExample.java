import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonExample {

  public static class User {
    public String name;
    public int id;
  }

  public static void main(String[] args) {
    ObjectMapper objectMapper = new ObjectMapper();

    User user = new User();
    user.name = "Matteo Antony";
    user.id = 123;

    try {
      String jsonString = objectMapper.writeValueAsString(user);
      System.out.println("JSON Serialization (string): " + jsonString + ", length: " + jsonString.getBytes().length);

      User deserializedUser = objectMapper.readValue(jsonString, User.class);
      System.out.println("Name deserialization: " + deserializedUser.name);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }
}
