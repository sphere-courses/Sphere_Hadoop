import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

class URLFilter{
    private static final String prefix = "Disallow: ";
    private List<Pattern> rules = new ArrayList<>();

    URLFilter(){

    }

    URLFilter(String ruleLine){
        if(ruleLine.length() == 0) return;

        String[] rulesStrings = ruleLine.split("\n");

        for(String ruleString : rulesStrings){
            ruleString = ruleString.substring(prefix.length());

            if(ruleString.startsWith("/")){
                ruleString = "^\\Q" + ruleString + "\\E.*";
            } else if(ruleString.startsWith("*")){
                ruleString = ruleString.substring(1);
                ruleString = ".*\\Q" + ruleString + "\\E.*";
            } else if(ruleLine.endsWith("S")){
                ruleString = ".*\\Q" + ruleString + "\\E$";
            }

            if (ruleString.endsWith("$\\E.*")) {
                ruleString = ruleString.substring(0, ruleString.length() - 5) + "\\E$";
            }

            rules.add(Pattern.compile(ruleString));
        }
    }

    boolean IsAllowed(String url){
        for(Pattern rule : rules){
            if(rule.matcher(url).matches()){
                return false;
            }
        }

        return true;
    }
}