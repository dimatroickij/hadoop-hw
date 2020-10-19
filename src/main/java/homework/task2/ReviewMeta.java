package homework.task2;

import com.google.gson.annotations.SerializedName;

public class ReviewMeta {

    @SerializedName("asin")
    private String prodId;

    @SerializedName("title")
    private String title;

    public ReviewMeta(String prodId, String title) {
        this.prodId = prodId;
        this.title = title;
    }

    public String getProdId() {
        return prodId;
    }

    public String getTitle() {
        return title;
    }
}
