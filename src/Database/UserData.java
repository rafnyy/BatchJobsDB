package Database;

/**
 *
 * @author Rob Fusco
 */
public class UserData {
    private String name;
    private String job_title;
    private String industry;
    long version;
    
    public enum FieldType {
        name(String.class),
        job_title(String.class),
        industry(String.class),
        version(long.class);

        private final Class clazz;

        private FieldType(Class clazz) {
            this.clazz = clazz;
        }

        public Class getFieldClass() {
            return clazz;
        }
    };
    
    public UserData(String name, String job_title, String industry) {
        this.name = name;
        this.job_title = job_title;
        this.industry = industry;
        this.version = 1;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        this.version++;
    }

    public String getJobTitle() {
        return job_title;
    }

    public void setJobTitle(String job_title) {
        this.job_title = job_title;
        this.version++;
    }

    public String getIndustry() {
        return industry;
    }

    public void setIndustry(String industry) {
        this.industry = industry;
        this.version++;
    }

    public long getVersion() {
        return version;
    }
}