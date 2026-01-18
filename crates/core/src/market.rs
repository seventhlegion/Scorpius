use crate::MarketId;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Market {
    pub id: MarketId,
    pub base: String,
    pub quote: String,
}
