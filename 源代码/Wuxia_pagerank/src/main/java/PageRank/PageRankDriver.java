package PageRank;

public class PageRankDriver {
    private static int times = 20;
    public static void main(String[] args)throws Exception {
        String[] graphPath = {args[0],"./temp/PR0"};
        GraphBuilder.main(graphPath);
        String[] iterPath = {"./temp/PR0","./temp/PR1"};
        for(int i=0; i<times; i++) {
            iterPath[0] = "./temp/PR" + String.valueOf(i);
            iterPath[1] = "./temp/PR" + String.valueOf(i + 1);
            PageRankIter.main(iterPath);
        }
        String[] viewerPath = {"./temp/PR"+times,args[1]};
        PageRankViewer.main(viewerPath);
    }
}
