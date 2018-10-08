public class Main {
    public static void main(String[] args){
        System.out.println("Hello World!");
        Point point = new Point(0,0);
        Thread thread_1 = new Thread(new MyClass(point));
        Thread thread_2 = new Thread(new MyClass(point));

        Thread thread_3 = new Thread(new MyClass(point));
        Thread thread_4 = new Thread(new MyClass(point));
        thread_1.start();
        thread_2.start();
        thread_3.start();
        thread_4.start();
        try {
            thread_1.join();
            thread_2.join();
            thread_3.join();
            thread_4.join();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        System.out.println(point);
    }



}
