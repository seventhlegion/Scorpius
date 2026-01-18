use scorpius_core::Money;

#[derive(Clone, Debug)]
pub struct PriceLevel {
    pub price: Money,
    pub total_quantity: Money,
}
