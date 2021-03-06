/*
Copyright (c) 2003, Dennis M. Sosnoski
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.
 * Neither the name of JiBX nor the names of its contributors may be used
   to endorse or promote products derived from this software without specific
   prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package extras;

import java.util.ArrayList;

import org.jibx.extras.IdDefRefMapperBase;
import org.jibx.extras.IdRefMapperBase;

public class NameArray
{
	public Object[] m_names;
    public ArrayList m_references;
    
    public static class Name
    {
        public String m_id;
        public String m_first;
        public String m_last;
    }
    
    private static class RefMapper extends IdRefMapperBase
    {
        public RefMapper(String uri, int index, String name) {
            super(uri, index, name);
        }

        protected String getIdValue(Object item) {
            return ((Name)item).m_id;
        }
    }
    
    private static class DefRefMapper extends IdDefRefMapperBase
    {
        public DefRefMapper(String uri, int index, String name) {
            super(uri, index, name);
        }

        protected String getIdValue(Object item) {
            return ((Name)item).m_id;
        }
    }
}
