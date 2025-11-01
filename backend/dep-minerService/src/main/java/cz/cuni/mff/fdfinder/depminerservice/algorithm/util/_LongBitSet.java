package cz.cuni.mff.fdfinder.depminerservice.algorithm.util;

import org.apache.lucene.util.OpenBitSet;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A serializable extension of {@link OpenBitSet} that implements {@link Externalizable}.
 * @author Richard
 */
public class _LongBitSet extends OpenBitSet implements Externalizable{

    /**
     * Writes the internal state of the {@link OpenBitSet} to an output stream.
     *
     * @param out the {@link ObjectOutput} stream to write data to
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(bits);
        out.writeInt(wlen);
    }

    /**
     * Reads the internal state of the {@link OpenBitSet} from an input stream.
     *
     * @param in the {@link ObjectInput} stream to read data from
     * @throws IOException if an I/O error occurs
     * @throws ClassNotFoundException if the serialized class cannot be found
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bits = (long[]) in.readObject();
        wlen = in.readInt();
    }
    
    
}
