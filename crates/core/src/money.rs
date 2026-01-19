use std::ops::{Add, Mul, Sub};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Money {
    pub amount: i64,
    pub scale: u32,
}

impl Money {
    pub fn new(amount: i64, scale: u32) -> Self {
        Self { amount, scale }
    }
}

impl Add for Money {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        assert_eq!(self.scale, rhs.scale);
        Self::new(self.amount + rhs.amount, self.scale)
    }
}

impl Sub for Money {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        assert_eq!(self.scale, rhs.scale);
        Self::new(self.amount - rhs.amount, self.scale)
    }
}

impl Mul<i64> for Money {
    type Output = Self;

    fn mul(self, rhs: i64) -> Self::Output {
        Self::new(self.amount * rhs, self.scale)
    }
}
