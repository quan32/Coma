package quan.coma;

public class Result {
	private int id;
	private float value;
	
	Result(){
		this.id=0;
		this.value=0.0f;
	}
	Result(int id, float value){
		this.id=id;
		this.value = value;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public float getValue() {
		return value;
	}

	public void setValue(float value) {
		this.value = value;
	}
}
