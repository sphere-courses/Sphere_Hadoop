public class MyClass implements Runnable{
    Point point;

    MyClass(Point point_){
        point = point_;
    }

    public void run(){
        for(int i = 0; i < 2000000; ++i) {
            point.increment();
        }
    }
}