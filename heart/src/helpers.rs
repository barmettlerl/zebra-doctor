use rand::Rng;


pub fn with_percentage_true(x: f64) -> bool {
    if !(0.0..=100.0).contains(&x) {
        panic!("Percentage must be between 0 and 100");
    }

    let rand_value: f64 = rand::thread_rng().gen_range(0.0..100.0);
    rand_value < x
}