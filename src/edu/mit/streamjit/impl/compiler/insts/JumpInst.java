package edu.mit.streamjit.impl.compiler.insts;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.BasicBlock;
import edu.mit.streamjit.impl.compiler.Value;

/**
 * An unconditional jump.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/11/2013
 */
public class JumpInst extends TerminatorInst {
	public JumpInst(BasicBlock target) {
		super(target.getType().getTypeFactory(), 1);
		setOperand(0, target);
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkArgument(i == 0, i);
		checkArgument(v instanceof BasicBlock, v.toString());
		super.checkOperand(i, v);
	}
}
