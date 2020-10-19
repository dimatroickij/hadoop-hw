package homework.task1;

import com.google.gson.annotations.SerializedName;

public class Review {

    @SerializedName("asin")
    private String prodId;

    @SerializedName("overall")
    private Float rating;

    public Review(String prodId, Float rating) {
        this.prodId = prodId;
        this.rating = rating;
    }

    public String getProdId() {
        return prodId;
    }

    public Float getRating() {
        return rating;
    }
}
