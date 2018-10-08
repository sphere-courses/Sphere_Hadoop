public class Point {
    int x, y;
    Point(int x_, int y_){
        x = x_;
        y = y_;
    }

    public void increment(){
        x++;
        y++;
    }

    public String toString(){
        return x + ", " + y;
    }
}
