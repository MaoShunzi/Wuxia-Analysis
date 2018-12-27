package LPA;

public class LPADriver {
    private static int times = 11;
    public static void main(String[] args)throws Exception {
        String[] initPath = {args[0],"./temp/LPA0"};
        InitLabel.main(initPath);
        String[] LPAPath = {"./temp/LPA0","./temp/LPA1"};
        for(int i=0; i<times; i++) {
            LPAPath[0] = "./temp/LPA" + String.valueOf(i);
            LPAPath[1] = "./temp/LPA" + String.valueOf(i + 1);
            LPA.main(LPAPath);
        }
        String[] rankPath = {"./temp/LPA"+times, args[1]};
        LPARank.main(rankPath);
    }
}
