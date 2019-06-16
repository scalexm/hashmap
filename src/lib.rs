pub mod atomic_arc;
pub mod hash_map;

#[test]
fn test_atomic_arc() {
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

#[test]
fn test_hash_map() {
    use hash_map::HashMap;

    let x: HashMap<i32, i32> = HashMap::new();
    println!("{:?}", x.get(&0));
    for i in 0..=7 {
        x.insert(i, i * 8);
        println!("inserted {}", i);
    }
    for i in 0..=7 {
        println!("{:?}", x.get(&i));
    }
}
