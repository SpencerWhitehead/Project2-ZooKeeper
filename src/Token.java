/**
 * Spencer Whitehead, whites5
 * Partha Sarathi Mukherjee, mukhep
 */


/* Class to represent a token (file) in the system. */
public class Token {
    private String fname; // File name
    String contents = ""; // File contents

    public Token(String f) {
        this.fname = f;
    }

    /* Retrieve file name. */
    public String getFname() {return fname;}

    /* Retrieve contents of file. */
    public String getContents() {return contents;}

    /* Set contents of file. */
    public void setContents(String data) {contents = data;}

    /* Append data to file. */
    public void appendContents(String toAppend) {
        StringBuilder s = new StringBuilder();
        s.append(contents);
        s.append(toAppend);
        contents = s.toString();
    }
}
