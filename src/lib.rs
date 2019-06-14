pub mod atomic_arc;
//pub mod hash_map;

#[test]
fn test() {
    #[derive(Debug)]
    struct Foo(i32);

    impl Drop for Foo {
        fn drop(&mut self) {
            println!("drop {}", self.0);
        }
    }

    use atomic_arc::{Arc, AtomicArc};
    let x = AtomicArc::new(Arc::new(Foo(4)));
    let y = x.load();
    let z = y.clone();
    let xx = Arc::new(Foo(5));
    x.store(xx.clone());
    let new_x = AtomicArc::new(xx);

    println!("x -> {}", x.load().0);
    println!("new_x -> {}", new_x.load().0);
    println!("z: {}", z.0);
    println!("y: {}", y.0);

    let x = AtomicArc::new_nullable(None);
    println!("{:?}", x.load());
    x.store(Some(Arc::new(Foo(7))));
    println!("{:?}", x.load());
}
