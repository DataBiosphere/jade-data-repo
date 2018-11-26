package bio.terra.jade.metadata.dataaccess;

import javax.persistence.*;
import javax.validation.constraints.*;
import java.io.Serializable;

@Entity
@Table(name="jade.metadata.Study")
public class StudyDAO implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @NotNull
    protected String Name;

    public int get_Id(){
        return id;
    }
    public void set_Id(int id){
        this.id = id;
    }
    public String getName(){
        return Name;
    }
    public void setName(String Name){
        this.Name = Name;
    }
}
